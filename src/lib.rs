use std::path::Path;

use blober::BlobChunk;
use rand::seq::SliceRandom;
pub use storage::Storage;

pub mod blober;
pub mod hash_value;
pub mod storage;

pub struct BackupConfig<S: Storage> {
    key_for_root: Vec<u8>,
    storage: S,
}

pub const CHUNK_SIZE: usize = 1024 * 1024;

pub async fn backup<S: Storage>(config: BackupConfig<S>, file: &Path) -> anyhow::Result<()> {
    let file_contents = tokio::fs::read(&file).await?;
    let blob = blober::Blob::from_bytes(&file_contents, CHUNK_SIZE);

    // Store each chunk
    let mut offset = 0;
    for chunk in &blob.chunks {
        let chunk_hash = chunk.hash.0.as_bytes();
        if !config.storage.has(chunk_hash).await? {
            let chunk_data = &file_contents[offset..(offset + CHUNK_SIZE)];
            config.storage.put(chunk_hash, chunk_data).await?;
        }
        offset += CHUNK_SIZE;
    }

    // Store the blob metadata
    let blob_data = bincode::encode_to_vec(&blob, bincode::config::standard())?;
    config.storage.put(&config.key_for_root, &blob_data).await?;

    Ok(())
}

pub async fn restore<S: Storage>(
    config: BackupConfig<S>,
    output_path: &Path,
) -> anyhow::Result<()> {
    let blob_data = config.storage.get(&config.key_for_root).await?;
    let blob: blober::Blob = bincode::decode_from_slice(&blob_data, bincode::config::standard())?.0;

    let mut file_contents = Vec::with_capacity(blob.size());

    for chunk in &blob.chunks {
        let chunk_data = config.storage.get(chunk.hash.0.as_bytes()).await?;
        if !chunk.verify(&chunk_data) {
            anyhow::bail!("hash didn't match")
        }
        file_contents.extend_from_slice(&chunk_data);
    }

    tokio::fs::write(output_path, file_contents).await?;

    Ok(())
}

pub async fn check_changed<S: Storage>(
    config: BackupConfig<S>,
    file_to_backup: &Path,
) -> anyhow::Result<bool> {
    let file_contents = tokio::fs::read(file_to_backup).await?;
    let blob_data = config.storage.get(&config.key_for_root).await?;
    let blob: blober::Blob = bincode::decode_from_slice(&blob_data, bincode::config::standard())?.0;

    Ok(blob.verify(&file_contents))
}

pub async fn partial_verify<S: Storage>(config: BackupConfig<S>) -> anyhow::Result<bool> {
    let blob_data = config.storage.get(&config.key_for_root).await?;
    let blob: blober::Blob = bincode::decode_from_slice(&blob_data, bincode::config::standard())?.0;

    let mut rng = rand::thread_rng();
    let chunks_to_check: Box<dyn Iterator<Item = &BlobChunk>> = if blob.chunks.len() < 10 {
        Box::new(blob.chunks.iter())
    } else {
        Box::new(blob.chunks.choose_multiple(&mut rng, 10))
    };
    for chunk in chunks_to_check {
        let chunk_data = config.storage.get(chunk.hash.0.as_bytes()).await?;
        if !chunk.verify(&chunk_data) {
            return Ok(false);
        }
    }

    Ok(true)
}
