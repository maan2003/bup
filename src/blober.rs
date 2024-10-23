use std::collections::BTreeSet;

use bincode::{Decode, Encode};

use crate::hash_value::HashValue;

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Decode, Encode)]
pub struct BlobChunk {
    pub(crate) hash: HashValue,
}

impl BlobChunk {
    pub fn from_bytes(data: &[u8]) -> BlobChunk {
        let hash = blake3::hash(chunk);
        BlobChunk {
            hash: HashValue(hash),
        }
    }

    pub fn verify(&self, data: &[u8]) -> bool {
        blake3::hash(data) == self.hash.0
    }
}

// TODO: handle multiple versions
// TODO(versioning): does the size of the blob change over time?
#[derive(Decode, Encode)]
pub struct Blob {
    pub(crate) chunks: Vec<BlobChunk>,
}

#[derive(Decode, Encode, Default)]
pub struct HashStore {
    pub(crate) chunks: BTreeSet<BlobChunk>,
}

impl HashStore {
    pub fn add(&mut self, chunk: BlobChunk) {
        self.chunks.insert(chunk);
    }

    pub fn contains(&self, chunk: &BlobChunk) -> bool {
        self.chunks.contains(chunk)
    }
}
