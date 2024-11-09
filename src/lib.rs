#![allow(dead_code)]
pub mod blob;
pub mod hash_value;
pub mod storage;

use bincode::{Decode, Encode};
use futures::executor::block_on;
use hash_value::HashValue;
use std::io::{ErrorKind, Read, Write};
use std::path::Path;
use std::sync::Arc;
use storage::Storage;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

pub const CHUNK_SIZE: usize = 512 * 1024;

use blob::{Blob, Document};

// FIXME: figure out when to remove blobs from the server
// we might need to track the blobs available on the server (locally)
// we could also use the list endpoint

#[derive(Debug, Clone)]
struct Block {
    idx: usize,
    data: Vec<u8>,
}

struct BlockUploader {
    storage: Storage,
    hash_rx: mpsc::Receiver<(blake3::Hash, Block)>,
}

impl BlockUploader {
    fn new(storage: Storage, hash_rx: mpsc::Receiver<(blake3::Hash, Block)>) -> Self {
        Self { storage, hash_rx }
    }

    async fn upload(&mut self, initial: bool) -> anyhow::Result<()> {
        let (mut blob, doc) = if !initial {
            let doc = self.storage.get_root_metadata().await?;
            (doc.current.clone(), Some(doc))
        } else {
            (Blob::default(), None)
        };

        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(16));
        while let Some((hash, block)) = self.hash_rx.recv().await {
            if blob.chunk_hashes.len() <= block.idx {
                blob.chunk_hashes.resize(block.idx + 1, HashValue(hash));
            } else {
                blob.chunk_hashes[block.idx] = HashValue(hash);
            }
            let storage = self.storage.clone();
            let permit = semaphore.clone().acquire_owned().await?;
            join_set.spawn(async move {
                let _permit = permit;
                storage.put_block(&hash, block.data).await
            });
        }

        while let Some(result) = join_set.join_next().await {
            result??;
        }

        let doc = match doc {
            Some(doc) => {
                doc.update(blob);
                doc
            }
            None => Document::new(blob),
        };

        self.storage.put_root_metadata(doc).await?;
        Ok(())
    }
}

const HASH_CHANNEL_SIZE: usize = 400;
pub async fn backup(storage: Storage, file: &Path, initial: bool) -> anyhow::Result<()> {
    let (hash_tx, hash_rx) = mpsc::channel::<(blake3::Hash, Block)>(HASH_CHANNEL_SIZE);
    let file_path = file.to_owned();
    let block_reader = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::open(file_path)?;

            for idx in 0.. {
                let hash_permit = block_on(hash_tx.clone().reserve_owned()).unwrap();
                let mut buffer = vec![0; CHUNK_SIZE];
                match file.read_exact(&mut buffer) {
                    Ok(()) => {
                        let block = Block { idx, data: buffer };
                        // FIXME: add semaphore to control the memory used
                        rayon::spawn_fifo(move || {
                            let hash = blake3::hash(&block.data);
                            hash_permit.send((hash, block));
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

    let mut uploader = BlockUploader::new(storage.clone(), hash_rx);
    let upload_task = tokio::spawn(async move { uploader.upload(initial).await });

    // Wait for all tasks to complete
    let (block_result, upload_result) = tokio::try_join!(block_reader, upload_task)?;
    block_result?;
    upload_result?;

    Ok(())
}

pub async fn restore(storage: Storage, output_path: &Path) -> anyhow::Result<()> {
    const CHANNEL_SIZE: usize = 400;
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
