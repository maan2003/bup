use std::path::Path;
use std::sync::Arc;

use bincode::{Decode, Encode};
use blober::BlobChunk;
use bytes::Bytes;
use object_store::{path::Path as ObjectStorePath, ObjectStore};

pub mod blober;
pub mod hash_value;

pub const CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Encode, Clone, Decode)]
pub struct Blob {
    pub chunks: Vec<BlobChunk>,
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
pub async fn backup<S: ObjectStore>(
    storage: Arc<S>,
    file: &Path,
    root_key: &str,
) -> anyhow::Result<()> {
    // FIXME: mmap
    // TODO: io uring might be better than mmap to avoid page faults hurting performance of hashing threads
    // probably makes sense to hash and network at same time
    let file_contents = tokio::fs::read(&file).await?;
    // FIXME: implement this
    let blob = Blob::from_bytes(&file_contents, CHUNK_SIZE);

    // Store each chunk
    let mut offset = 0;
    for chunk in &blob.chunks {
        let chunk_path = ObjectStorePath::from(hex::encode(chunk.hash.0.as_bytes()));

        // Check if chunk exists using head()
        if storage.head(&chunk_path).await.is_err() {
            let chunk_data = &file_contents[offset..(offset + CHUNK_SIZE)];
            storage
                .put(&chunk_path, Bytes::copy_from_slice(chunk_data).into())
                .await?;
        }
        offset += CHUNK_SIZE;
    }

    // Store the blob metadata
    let blob_data = bincode::encode_to_vec(&blob, bincode::config::standard())?;
    let root_path = ObjectStorePath::from(root_key);
    storage
        .put(&root_path, Bytes::from(blob_data).into())
        .await?;

    Ok(())
}

pub async fn restore<S: ObjectStore>(
    storage: Arc<S>,
    output_path: &Path,
    root_key: &str,
) -> anyhow::Result<()> {
    let root_path = ObjectStorePath::from(root_key);
    let get_result = storage.get(&root_path).await?;
    let blob_data = get_result.bytes().await?;
    let blob: blober::Blob = bincode::decode_from_slice(&blob_data, bincode::config::standard())?.0;

    let mut file_contents = Vec::new();

    for chunk in &blob.chunks {
        let chunk_path = ObjectStorePath::from(hex::encode(chunk.hash.0.as_bytes()));
        let chunk_result = storage.get(&chunk_path).await?;
        let chunk_data = chunk_result.bytes().await?;

        if !chunk.verify(&chunk_data) {
            anyhow::bail!("hash didn't match")
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

        backup(storage.clone(), temp_file.path(), root_key).await?;

        let output_file = NamedTempFile::new()?;
        restore(storage, output_file.path(), root_key).await?;

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
        backup(storage.clone(), temp_file.path(), root_key).await?;

        // Modify file
        let updated_content = b"Updated content".repeat(1024 * 1024);
        tokio::fs::write(temp_file.path(), &updated_content).await?;

        // Update backup
        backup(storage.clone(), temp_file.path(), root_key).await?;

        // Restore and verify updated content
        let output_file = NamedTempFile::new()?;
        restore(storage, output_file.path(), root_key).await?;

        let restored_content = tokio::fs::read(output_file.path()).await?;
        assert_eq!(updated_content.to_vec(), restored_content);

        Ok(())
    }
}
