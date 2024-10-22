use bincode::{Decode, Encode};
use std::collections::BTreeSet;

mod file_chunk_impl;

#[cfg(test)]
mod tests;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FileChunk {
    hash: blake3::Hash,
}

#[derive(Decode, Encode)]
pub struct FileContent {
    chunk_size: usize,
    chunks: Vec<FileChunk>,
}

impl FileContent {
    /// Returns the total size of the file content in bytes
    pub fn size(&self) -> usize {
        self.chunk_size * self.chunks.len()
    }

    /// The caller should ensure that the input bytes are zero-padded to make the total length
    pub fn from_bytes(bytes: &[u8], chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "Chunk size must be greater than zero");
        assert!(
            bytes.len() % chunk_size == 0,
            "Input bytes length must be a multiple of chunk_size"
        );

        let mut chunks = Vec::new();

        for chunk in bytes.chunks(chunk_size) {
            let hash = blake3::hash(chunk);
            chunks.push(FileChunk { hash });
        }

        FileContent { chunk_size, chunks }
    }

    /// The caller should ensure that the input bytes are zero-padded to make the total length
    pub fn verify(&self, bytes: &[u8]) -> bool {
        if bytes.len() != self.chunk_size * self.chunks.len() {
            return false;
        }

        for (chunk, file_chunk) in bytes.chunks(self.chunk_size).zip(self.chunks.iter()) {
            if blake3::hash(chunk) != file_chunk.hash {
                return false;
            }
        }

        true
    }

    pub fn add_to_hash_store(&self, store: &mut HashStore) {
        for chunk in &self.chunks {
            store.add(chunk.clone());
        }
    }
}

#[derive(Decode, Encode, Default)]
pub struct HashStore {
    chunks: BTreeSet<FileChunk>,
}

impl HashStore {
    pub fn add(&mut self, chunk: FileChunk) {
        self.chunks.insert(chunk);
    }

    pub fn contains(&self, chunk: &FileChunk) -> bool {
        self.chunks.contains(chunk)
    }
}
