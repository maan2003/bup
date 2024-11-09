use crate::hash_value::HashValue;
use bincode::{Decode, Encode};

#[derive(Encode, Clone, Decode, Debug, PartialEq)]
pub struct Blob {
    chunk_hashes: Vec<HashValue>,
    timestamp: i64,
}

// Used to store all version info for a backed up file
#[derive(Encode, Clone, Decode, Debug)]
pub struct Document {
    current: Blob,
    history: Vec<PrevBlob>,
}

// Stores differences between consecutive versions
#[derive(Encode, Clone, Decode, Debug)]
pub struct PrevBlob {
    // Each number represents how many chunks to copy from next version before using a diff chunk
    same_chunks_lengths: Vec<usize>,
    // Different chunks to insert between runs of same chunks
    diff_chunks: Vec<HashValue>,
    timestamp: i64,
}

impl Document {
    pub fn new(blob: Blob) -> Self {
        Self {
            current: blob,
            history: Vec::new(),
        }
    }
    pub fn current(&self) -> &Blob {
        &self.current
    }
    pub fn update(&mut self, new_blob: Blob) {
        new_blob.verify_invariants();
        // Create diff from current version
        let prev_blob = PrevBlob::from_diff(&new_blob, &self.current);
        self.history.push(prev_blob);
        self.current = new_blob;
    }

    pub fn get_version(&self, version: usize) -> Option<Blob> {
        if version >= self.history.len() + 1 {
            return None;
        }

        if version == self.history.len() {
            return Some(self.current().clone());
        }

        // We need to reconstruct the version by applying diffs in reverse
        let mut current = self.current.clone();
        for prev_blob in self.history[version..].iter().rev() {
            current = prev_blob.compute(&current);
        }
        Some(current)
    }
}

const FAKE_HASH: blake3::Hash = blake3::Hash::from_bytes([0; 32]);
impl Blob {
    pub fn empty() -> Self {
        Self {
            chunk_hashes: Vec::new(),
            timestamp: chrono::Utc::now().timestamp(),
        }
    }

    pub fn set(&mut self, idx: usize, hash: blake3::Hash) {
        if self.chunk_hashes.len() <= idx {
            self.chunk_hashes.resize(idx + 1, HashValue(FAKE_HASH));
        }
        self.chunk_hashes[idx] = HashValue(hash);
    }
    pub fn chunk_hashes(&self) -> &[HashValue] {
        &self.chunk_hashes
    }
    pub fn verify_invariants(&self) {
        assert!(self.chunk_hashes().iter().all(|x| x.0 != FAKE_HASH));
    }
}

impl PrevBlob {
    // Create a diff between two versions
    pub fn from_diff(current: &Blob, prev: &Blob) -> Self {
        let mut same_chunks_lengths = Vec::new();
        let mut diff_chunks = Vec::new();

        let mut current_same_run = 0;

        for i in 0..prev.chunk_hashes.len() {
            if i < current.chunk_hashes.len() && current.chunk_hashes[i] == prev.chunk_hashes[i] {
                current_same_run += 1;
            } else {
                same_chunks_lengths.push(current_same_run);
                current_same_run = 0;
                diff_chunks.push(prev.chunk_hashes[i]);
            }
        }

        if current_same_run > 0 {
            same_chunks_lengths.push(current_same_run);
        }

        let result = Self {
            same_chunks_lengths,
            diff_chunks,
            timestamp: prev.timestamp,
        };

        // atmost 1 different
        assert!(result.same_chunks_lengths.len() - result.diff_chunks.len() < 2);
        assert_eq!(
            result.compute(current),
            *prev,
            "compute_version should reconstruct original"
        );

        result
    }

    // Reconstruct a full blob from a diff and next version
    pub fn compute(&self, next_version: &Blob) -> Blob {
        let mut next_chunks = next_version.chunk_hashes.iter();
        let mut diff_chunks = self.diff_chunks.iter();
        let mut chunks_hashes = Vec::new();

        for same_len in &self.same_chunks_lengths {
            // Copy same chunks from next version
            for _ in 0..*same_len {
                chunks_hashes.push(next_chunks.next().unwrap().clone());
            }

            // Add one different chunk
            if let Some(diff_chunk) = diff_chunks.next() {
                let _ = next_chunks.next();
                chunks_hashes.push(diff_chunk.clone());
            }
        }

        Blob {
            chunk_hashes: chunks_hashes,
            timestamp: self.timestamp,
        }
    }
}
