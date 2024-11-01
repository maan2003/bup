#![allow(dead_code)]
pub mod hash_value;
pub mod storage;

use bincode::{Decode, Encode};
use hash_value::HashValue;
use std::io::{ErrorKind, Read, Write};
use std::path::Path;
use storage::Storage;
use tokio::sync::mpsc;

pub const CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Encode, Clone, Decode, Debug)]
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
    idx: usize,
    data: Vec<u8>,
}

pub async fn backup(storage: Storage, file: &Path, initial: bool) -> anyhow::Result<()> {
    // Channel sizes - adjust based on testing
    const BLOCK_CHANNEL_SIZE: usize = 100;
    const HASH_CHANNEL_SIZE: usize = 100;

    // Create channels
    let (hash_tx, mut hash_rx) = mpsc::channel::<(blake3::Hash, Block)>(HASH_CHANNEL_SIZE);
    let file_path = file.to_owned();
    let block_reader = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::open(file_path)?;

            for idx in 0.. {
                let mut buffer = vec![0; CHUNK_SIZE];
                match file.read_exact(&mut buffer) {
                    Ok(()) => {
                        let block = Block { idx, data: buffer };
                        let hash_tx = hash_tx.clone();
                        // FIXME: add semaphore to control the memory used
                        rayon::spawn_fifo(move || {
                            let hash = blake3::hash(&block.data);
                            hash_tx.blocking_send((hash, block)).unwrap();
                        });
                    }
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e.into()),
                }
            }
            anyhow::Ok(())
        })
        .await??;

        Ok::<(), anyhow::Error>(())
    });

    let storage = storage.clone();
    let upload_task = tokio::spawn(async move {
        let mut blob: Blob = if initial {
            Blob {
                chunk_hashes: vec![],
            }
        } else {
            storage.get_root_metadata().await?
        };
        while let Some((hash, block)) = hash_rx.recv().await {
            if blob.chunk_hashes.len() <= block.idx {
                // *hopefully* block will replace all the placeholders, it is scary
                blob.chunk_hashes.resize(block.idx + 1, HashValue(hash));
            } else {
                blob.chunk_hashes[block.idx] = HashValue(hash);
            }
            // FIXME: concurrency
            storage.put_block(&hash, block.data).await?;
        }

        storage.put_root_metadata(&blob).await?;

        anyhow::Ok(())
    });

    // Wait for all tasks to complete
    let (block_result, upload_result) = tokio::try_join!(block_reader, upload_task)?;
    block_result?;
    upload_result?;

    Ok(())
}

pub async fn restore(storage: Storage, output_path: &Path) -> anyhow::Result<()> {
    const CHANNEL_SIZE: usize = 100;
    let (chunk_tx, mut chunk_rx) = mpsc::channel(CHANNEL_SIZE);

    let storage_clone = storage.clone();
    let fetch_task = tokio::spawn(async move {
        let blob: Blob = storage.get_root_metadata().await?;
        for chunk_hash in &blob.chunk_hashes {
            let chunk_data = storage_clone.get_block(&chunk_hash.0).await?;
            if chunk_hash.0 != blake3::hash(&chunk_data) {
                anyhow::bail!("hash didn't match, storage server error");
            }
            chunk_tx.send(chunk_data).await?;
        }
        anyhow::Ok(())
    });

    let output_path = output_path.to_owned();
    let write_task = tokio::task::spawn_blocking(move || {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(output_path)?;

        while let Some(chunk_data) = chunk_rx.blocking_recv() {
            use std::io::Write;
            file.write_all(&chunk_data)?;
        }

        file.flush()?;
        anyhow::Ok(())
    });

    let (fetch_result, write_result) = tokio::try_join!(fetch_task, write_task)?;
    fetch_result?;
    write_result?;

    Ok(())
}
