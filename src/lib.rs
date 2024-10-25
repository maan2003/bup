#![allow(dead_code)]
pub mod hash_value;

use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use base64::Engine;
use bincode::{Decode, Encode};
use hash_value::HashValue;
use object_store::ObjectStore;
use std::io::{ErrorKind, Read};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

type ObjectStorePath = object_store::path::Path;

pub const CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Encode, Clone, Decode)]
pub struct Blob {
    pub chunk_hashes: Vec<HashValue>,
}

// is it better to use multiple backup processes to backup multiple files
// TODO: is there a better way to figure out if the blob is changed than to just hashing all the blobs
// does lvm have a way of tracking if a blob got changed since last snapshot
// we need a pointer equal
// looks like there is thin_delta tool
// we can verify the thin delta by just doing

// TODO: parallelize
// TODO: what is good level of parallelization for requests
// TODO: figure out a good way to control the back pressure for hashing

// FIXME: figure out when to remove blobs from the server
// we might need to track the blobs available on the server (locally)
// we could also use the list endpoint

#[derive(Debug, Clone)]
struct Block {
    offset: u64,
    data: Vec<u8>,
}

fn object_path(key: &[u8]) -> ObjectStorePath {
    ObjectStorePath::from(BASE64_URL_SAFE_NO_PAD.encode(key))
}

#[allow(unused_variables)]
pub async fn backup<S: ObjectStore>(
    storage: Arc<S>,
    file: &Path,
    root_key: ObjectStorePath,
) -> anyhow::Result<()> {
    // Channel sizes - adjust based on testing
    const BLOCK_CHANNEL_SIZE: usize = 100;
    const HASH_CHANNEL_SIZE: usize = 100;

    // Create channels
    let (block_tx, mut block_rx) = mpsc::channel::<Block>(BLOCK_CHANNEL_SIZE);
    let (hash_tx, mut hash_rx) = mpsc::channel::<(blake3::Hash, Block)>(HASH_CHANNEL_SIZE);
    let file_path = file.to_owned();
    let block_reader = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::open(file_path)?;
            let mut offset = 0;

            loop {
                let mut buffer = vec![0; CHUNK_SIZE];
                match file.read_exact(&mut buffer) {
                    Ok(()) => {
                        let block = Block {
                            offset,
                            data: buffer,
                        };
                        block_tx.blocking_send(block)?;
                        offset += CHUNK_SIZE as u64;
                    }
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => break anyhow::Ok(()),
                    Err(e) => return Err(e.into()),
                }
            }
        })
        .await??;

        Ok::<(), anyhow::Error>(())
    });

    let hash_task = tokio::spawn(async move {
        while let Some(block) = block_rx.recv().await {
            let hash_tx = hash_tx.clone();
            rayon::spawn_fifo(move || {
                let hash = blake3::hash(&block.data);
                hash_tx.blocking_send((hash, block)).unwrap();
            });
        }
        anyhow::Ok(())
    });

    let upload_task = tokio::spawn(async move {
        // FIXME: actually adjust the current blocks
        let chunk_hashes = Vec::new();

        while let Some((hash, block)) = hash_rx.recv().await {
            // FIXME: concurrency
            info!("uploading block");

            let object_path = object_path(hash.as_bytes());
            match storage.head(&object_path).await {
                Ok(_) => {
                    info!("object_store already has existing block content");
                }
                Err(object_store::Error::NotFound { .. }) | Err(_) => {
                    info!("object_store uploading the block content");
                    storage.put(&object_path, block.data.into()).await?;
                }
            }
        }

        // Create and store blob metadata
        let blob = Blob { chunk_hashes };
        let blob_data = bincode::encode_to_vec(&blob, bincode::config::standard())?;
        storage.put(&root_key, blob_data.into()).await?;

        anyhow::Ok(())
    });

    // Wait for all tasks to complete
    let (_, hash_result, upload_result) = tokio::try_join!(block_reader, hash_task, upload_task)?;
    hash_result?;
    upload_result?;

    Ok(())
}

pub async fn restore<S: ObjectStore>(
    storage: Arc<S>,
    output_path: &Path,
    root_path: ObjectStorePath,
) -> anyhow::Result<()> {
    let get_result = storage.get(&root_path).await?;
    let blob_data = get_result.bytes().await?;
    let blob: Blob = bincode::decode_from_slice(&blob_data, bincode::config::standard())?.0;

    let mut file_contents = Vec::new();

    for chunk_hash in &blob.chunk_hashes {
        let chunk_path = object_path(chunk_hash.0.as_bytes());
        let chunk_result = storage.get(&chunk_path).await?;
        let chunk_data = chunk_result.bytes().await?;

        if chunk_hash.0 != blake3::hash(&chunk_data) {
            anyhow::bail!("hash didn't match, storage server error")
        }
        file_contents.extend_from_slice(&chunk_data);
    }

    tokio::fs::write(output_path, file_contents).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::io::Write;
    use tempfile::NamedTempFile;

    async fn create_temp_file(content: &[u8]) -> anyhow::Result<NamedTempFile> {
        let mut temp_file = NamedTempFile::new()?;
        temp_file.write_all(content)?;
        temp_file.flush()?;
        Ok(temp_file)
    }

    #[tokio::test]
    async fn test_backup_and_restore() -> anyhow::Result<()> {
        let content = b"Hello, World!".repeat(1024 * 1024);
        let temp_file = create_temp_file(&content).await?;
        let storage = Arc::new(InMemory::new());
        let root_key = "root";

        backup(storage.clone(), temp_file.path(), root_key.into()).await?;

        let output_file = NamedTempFile::new()?;
        restore(storage, output_file.path(), root_key.into()).await?;

        let restored_content = tokio::fs::read(output_file.path()).await?;
        assert_eq!(content.to_vec(), restored_content);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_backup() -> anyhow::Result<()> {
        let initial_content = b"Initial content".repeat(1024 * 1024);
        let temp_file = create_temp_file(&initial_content).await?;
        let storage = Arc::new(InMemory::new());
        let root_key = "root";

        // Initial backup
        backup(storage.clone(), temp_file.path(), root_key.into()).await?;

        // Modify file
        let updated_content = b"Updated content".repeat(1024 * 1024);
        tokio::fs::write(temp_file.path(), &updated_content).await?;

        // Update backup
        backup(storage.clone(), temp_file.path(), root_key.into()).await?;

        // Restore and verify updated content
        let output_file = NamedTempFile::new()?;
        restore(storage, output_file.path(), root_key.into()).await?;

        let restored_content = tokio::fs::read(output_file.path()).await?;
        assert_eq!(updated_content.to_vec(), restored_content);

        Ok(())
    }
}
