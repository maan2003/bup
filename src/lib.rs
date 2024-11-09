#![allow(dead_code)]
pub mod blob;
pub mod storage;

#[cfg(test)]
mod tests;

use anyhow::Context;
use futures::executor::block_on;
use std::collections::BTreeSet;
use std::io::{ErrorKind, Read, Write};
use std::path::Path;
use std::sync::Arc;
use storage::Storage;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tracing::info;

// 512kb
pub const CHUNK_SIZE: usize = 512 * 1024;

use blob::{Blob, Document};

// FIXME: figure out when to remove blobs from the server
// we might need to track the blobs available on the server (locally)
// we could also use the list endpoint

const HASH_CHANNEL_SIZE: usize = 400;
pub async fn backup(storage: Storage, file: &Path) -> anyhow::Result<()> {
    #[derive(Debug, Clone)]
    struct Chunk {
        idx: usize,
        data: Vec<u8>,
    }
    let (hash_tx, mut hash_rx) = mpsc::channel::<(blake3::Hash, Chunk)>(HASH_CHANNEL_SIZE);
    let file_path = file.to_owned();
    let chunk_reader = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::open(file_path)?;

            for idx in 0.. {
                let hash_permit = block_on(hash_tx.clone().reserve_owned()).unwrap();
                let mut buffer = vec![0; CHUNK_SIZE];
                match file.read_exact(&mut buffer) {
                    Ok(()) => {
                        let chunk = Chunk { idx, data: buffer };
                        rayon::spawn_fifo(move || {
                            let hash = blake3::hash(&chunk.data);
                            hash_permit.send((hash, chunk));
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
        let (doc, available_hashes) =
            tokio::try_join!(storage.get_root_metadata(), storage.available_hashes())?;
        let mut new_blob = doc
            .as_ref()
            .map_or_else(Blob::empty, |d| d.current().fork());
        let mut hashes_sent = available_hashes
            .into_iter()
            .map(<[u8; 32]>::from)
            .collect::<BTreeSet<_>>();

        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(16));

        while let Some((hash, chunk)) = hash_rx.recv().await {
            new_blob.set(chunk.idx, hash);
            if hashes_sent.insert(hash.into()) {
                let permit = semaphore.clone().acquire_owned().await?;
                let storage = storage.clone();
                join_set.spawn(async move {
                    let _permit = permit;
                    info!(idx = chunk.idx, "Uploading chunk");
                    storage.put_chunk(&hash, chunk.data).await
                });
            }
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

        storage.put_root_metadata(doc).await?;
        anyhow::Ok(())
    });

    // Wait for all tasks to complete
    let (chunk_result, upload_result) = tokio::try_join!(chunk_reader, upload_task)?;
    chunk_result?;
    upload_result?;

    Ok(())
}

pub async fn restore(storage: Storage, output_path: &Path) -> anyhow::Result<()> {
    const CHANNEL_SIZE: usize = 400;
    let (chunk_tx, mut chunk_rx) = mpsc::channel(CHANNEL_SIZE);

    let storage_clone = storage.clone();
    let fetch_task = tokio::spawn(async move {
        let doc: Document = storage
            .get_root_metadata()
            .await?
            .context("root metadata not found")?;
        for chunk_hash in doc.current().chunk_hashes() {
            let chunk_data = storage_clone.get_chunk(&chunk_hash).await?;
            if chunk_hash != blake3::hash(&chunk_data) {
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
