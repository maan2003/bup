#![allow(dead_code)]
pub mod blob;
pub mod hash_value;
pub mod storage;

use futures::executor::block_on;
use std::io::{ErrorKind, Read, Write};
use std::path::Path;
use std::sync::Arc;
use storage::Storage;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

// 512kb
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

const HASH_CHANNEL_SIZE: usize = 400;
pub async fn backup(storage: Storage, file: &Path, initial: bool) -> anyhow::Result<()> {
    let (hash_tx, mut hash_rx) = mpsc::channel::<(blake3::Hash, Block)>(HASH_CHANNEL_SIZE);
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

    let storage_for_upload = storage.clone();
    let upload_task = tokio::spawn(async move {
        let (mut new_blob, doc) = if !initial {
            let doc = storage_for_upload.get_root_metadata().await?;
            (doc.current().clone(), Some(doc))
        } else {
            (Blob::empty(), None)
        };

        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(16));

        while let Some((hash, block)) = hash_rx.recv().await {
            new_blob.set(block.idx, hash);
            let storage = storage_for_upload.clone();
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
            Some(mut doc) => {
                doc.update(new_blob);
                doc
            }
            None => Document::new(new_blob),
        };

        storage_for_upload.put_root_metadata(doc).await?;
        anyhow::Ok(())
    });

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
        let doc: Document = storage.get_root_metadata().await?;
        for chunk_hash in doc.current().chunk_hashes() {
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
