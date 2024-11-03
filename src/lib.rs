#![allow(dead_code)]
pub mod hash_value;
pub mod storage;

use bincode::{Decode, Encode};
use futures::executor::block_on;
use hash_value::HashValue;
use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use storage::Storage;
use thinp::commands::engine::*;
use thinp::commands::utils::mk_report;
use thinp::thin::delta::{self, ThinDeltaOptions};
use thinp::thin::delta_visitor::{Delta, DeltaVisitor, Snap};
use thinp::thin::ir::{self, Visit};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tracing::error;

pub const CHUNK_SIZE: usize = 128 * 1024;

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

struct BlockUploader {
    storage: Storage,
    hash_rx: mpsc::Receiver<(blake3::Hash, Block)>,
}

impl BlockUploader {
    fn new(storage: Storage, hash_rx: mpsc::Receiver<(blake3::Hash, Block)>) -> Self {
        Self { storage, hash_rx }
    }

    async fn upload(&mut self, initial: bool) -> anyhow::Result<()> {
        let mut blob: Blob = if initial {
            Blob {
                chunk_hashes: vec![],
            }
        } else {
            self.storage.get_root_metadata().await?
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

        self.storage.put_root_metadata(&blob).await?;
        Ok(())
    }
}

struct BlockDeltaVisitor {
    block_hash_tx: mpsc::Sender<(blake3::Hash, Block)>,
    snapshot_file: std::fs::File,
    data_block_size_bytes: Option<u64>,
    processed_queue: VecDeque<u64>,
}

impl BlockDeltaVisitor {
    fn new(
        block_hash_tx: mpsc::Sender<(blake3::Hash, Block)>,
        snapshot_file: std::fs::File,
    ) -> Self {
        Self {
            block_hash_tx,
            snapshot_file,
            data_block_size_bytes: None,
            processed_queue: VecDeque::new(),
        }
    }

    fn process_block(&mut self, thin_begin: u64) -> anyhow::Result<()> {
        let data_block_size_bytes = self
            .data_block_size_bytes
            .expect("data_block_size must be set from superblock");
        let offset = thin_begin * data_block_size_bytes;

        let chunk_idx = offset / CHUNK_SIZE as u64;
        let chunk_start = chunk_idx * CHUNK_SIZE as u64;

        if self.processed_queue.contains(&chunk_idx) {
            return Ok(());
        }
        self.processed_queue.push_back(chunk_idx);
        if self.processed_queue.len() > 100 {
            self.processed_queue.pop_front();
        }
        let snapshot_file = self.snapshot_file.try_clone()?;
        let hash_tx = self.block_hash_tx.clone();

        tokio::task::spawn_blocking(move || {
            let hash_permit = block_on(hash_tx.reserve_owned()).unwrap();
            let mut buffer = vec![0; CHUNK_SIZE];
            if let Err(err) = snapshot_file.read_exact_at(&mut buffer, chunk_start) {
                error!(%chunk_idx, %err, "failed to read chunk");
                return;
            }

            let block = Block {
                idx: chunk_idx.try_into().unwrap(),
                data: buffer,
            };

            rayon::spawn_fifo(move || {
                let hash = blake3::hash(&block.data);
                hash_permit.send((hash, block));
            });
        });

        Ok(())
    }
}

impl DeltaVisitor for BlockDeltaVisitor {
    fn superblock_b(&mut self, sb: &ir::Superblock) -> anyhow::Result<Visit> {
        self.data_block_size_bytes = Some(sb.data_block_size as u64 * 1024); // kb to b
        Ok(Visit::Continue)
    }
    fn delta(&mut self, d: &Delta) -> anyhow::Result<Visit> {
        match d {
            Delta::LeftOnly(m) | Delta::RightOnly(m) => self.process_block(m.thin_begin)?,
            Delta::Differ(m) => self.process_block(m.thin_begin)?,
            Delta::Same(_) => (),
        }
        Ok(Visit::Continue)
    }
}

pub async fn backup(storage: Storage, file: &Path, initial: bool) -> anyhow::Result<()> {
    const HASH_CHANNEL_SIZE: usize = 400;

    // Create channels
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

pub async fn backup_lvm_thin(
    storage: Storage,
    snapshot_file: &Path,
    snap_id1: u64,
    snap_id2: u64,
    meta_file: &Path,
) -> anyhow::Result<()> {
    const HASH_CHANNEL_SIZE: usize = 400;
    let (block_hash_tx, hash_rx) = mpsc::channel::<(blake3::Hash, Block)>(HASH_CHANNEL_SIZE);

    let snapshot_path = snapshot_file.to_owned();
    let meta_path = meta_file.to_owned();
    let block_reader = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            let snapshot_file = std::fs::File::open(snapshot_path)?;
            let mut visitor = BlockDeltaVisitor::new(block_hash_tx, snapshot_file);

            let engine_opts = EngineOptions {
                tool: ToolType::Thin,
                engine_type: EngineType::Sync,
                use_metadata_snap: true,
            };

            let opts = ThinDeltaOptions {
                input: &meta_path,
                engine_opts,
                report: mk_report(true),
                snap1: Snap::DeviceId(snap_id1),
                snap2: Snap::DeviceId(snap_id2),
                verbose: false,
            };
            delta::delta_with_visitor(opts, &mut visitor)?;
            anyhow::Ok(())
        })
        .await??;
        Ok::<(), anyhow::Error>(())
    });

    let mut uploader = BlockUploader::new(storage.clone(), hash_rx);
    let upload_task = tokio::spawn(async move { uploader.upload(false).await });

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
