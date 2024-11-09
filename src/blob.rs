use crate::CHUNK_SIZE;
use bincode::{Decode, Encode};
use chrono::{DateTime, Utc};

#[derive(Encode, Clone, Decode, Debug, PartialEq)]
pub struct Blob {
    chunk_hashes: Vec<[u8; 32]>,
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
    same_chunks_lengths: Vec<usize>,
    diff_chunks: Vec<[u8; 32]>,
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
    pub fn versions(&self) -> impl Iterator<Item = &PrevBlob> + '_ {
        self.history.iter().rev()
    }
}

const FAKE_HASH: [u8; 32] = [0; 32];
impl Blob {
    pub fn empty() -> Self {
        Self {
            chunk_hashes: Vec::new(),
            timestamp: chrono::Utc::now().timestamp(),
        }
    }
    pub fn timestamp(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.timestamp, 0).unwrap()
    }
    pub fn size(&self) -> u64 {
        self.chunk_hashes.len() as u64 * CHUNK_SIZE as u64
    }
    pub fn fork(&self) -> Self {
        let mut this = self.clone();
        this.timestamp = chrono::Utc::now().timestamp();
        this
    }
    pub fn set(&mut self, idx: usize, hash: blake3::Hash) {
        if self.chunk_hashes.len() <= idx {
            self.chunk_hashes.resize(idx + 1, FAKE_HASH);
        }
        self.chunk_hashes[idx] = hash.into();
    }
    pub fn chunk_hashes(&self) -> impl Iterator<Item = blake3::Hash> + '_ {
        self.chunk_hashes
            .iter()
            .map(|x| blake3::Hash::from_bytes(*x))
    }
    pub fn verify_invariants(&self) {
        assert!(self.chunk_hashes.iter().all(|x| x != &FAKE_HASH));
    }
}

impl PrevBlob {
    // Create a diff between two versions
    fn from_diff(current: &Blob, prev: &Blob) -> Self {
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
    fn compute(&self, next_version: &Blob) -> Blob {
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
    pub fn retained_size(&self) -> u64 {
        self.diff_chunks.len() as u64 * CHUNK_SIZE as u64
    }
    pub fn timestamp(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.timestamp, 0).unwrap()
    }
    pub fn unique_chunk_hashes(&self) -> impl Iterator<Item = blake3::Hash> + '_ {
        self.diff_chunks
            .iter()
            .map(|x| blake3::Hash::from_bytes(*x))
    }
}
