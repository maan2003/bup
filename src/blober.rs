use std::collections::BTreeSet;

use bincode::{Decode, Encode};

use crate::hash_value::HashValue;

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Decode, Encode)]
pub struct BlobChunk {
    pub(crate) hash: HashValue,
}

impl BlobChunk {
    pub fn verify(&self, data: &[u8]) -> bool {
        blake3::hash(data) == self.hash.0
    }
}

#[derive(Decode, Encode)]
pub struct Blob {
    pub(crate) chunk_size: usize,
    pub(crate) chunks: Vec<BlobChunk>,
}

impl Blob {
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
            chunks.push(BlobChunk {
                hash: HashValue(hash),
            });
        }

        Blob { chunk_size, chunks }
    }

    /// The caller should ensure that the input bytes are zero-padded to make the total length
    pub fn verify(&self, bytes: &[u8]) -> bool {
        if bytes.len() != self.chunk_size * self.chunks.len() {
            return false;
        }

        for (chunk, file_chunk) in bytes.chunks(self.chunk_size).zip(self.chunks.iter()) {
            if !file_chunk.verify(chunk) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_bytes_and_verify() {
        let data = b"Hello, world! This is a test.".to_vec();
        let chunk_size = 8;
        let padded_data = pad_to_chunk_size(&data, chunk_size);

        let file_content = Blob::from_bytes(&padded_data, chunk_size);

        assert_eq!(file_content.chunk_size, chunk_size);
        assert_eq!(file_content.chunks.len(), padded_data.len() / chunk_size);
        assert!(file_content.verify(&padded_data));

        // Test with modified data
        let mut modified_data = padded_data.clone();
        modified_data[5] = b'X';
        assert!(!file_content.verify(&modified_data));
    }

    #[test]
    fn test_empty_input() {
        let data = vec![];
        let chunk_size = 4;
        let file_content = Blob::from_bytes(&data, chunk_size);

        assert_eq!(file_content.chunk_size, chunk_size);
        assert_eq!(file_content.chunks.len(), 0);
        assert!(file_content.verify(&data));
    }

    fn pad_to_chunk_size(data: &[u8], chunk_size: usize) -> Vec<u8> {
        let remainder = data.len() % chunk_size;
        if remainder == 0 {
            data.to_vec()
        } else {
            let padding = chunk_size - remainder;
            let mut padded = data.to_vec();
            padded.extend(vec![0; padding]);
            padded
        }
    }

    #[test]
    fn test_bincode_serialization() {
        let data = b"Hello, world! This is a test.".to_vec();
        let chunk_size = 8;
        let padded_data = pad_to_chunk_size(&data, chunk_size);

        let original_content = Blob::from_bytes(&padded_data, chunk_size);

        // Serialize
        let encoded: Vec<u8> =
            bincode::encode_to_vec(&original_content, bincode::config::standard()).unwrap();

        // Deserialize
        let decoded_content: Blob =
            bincode::decode_from_slice(&encoded, bincode::config::standard())
                .unwrap()
                .0;

        // Verify the deserialized content matches the original
        assert_eq!(original_content.chunk_size, decoded_content.chunk_size);
        assert_eq!(original_content.chunks.len(), decoded_content.chunks.len());

        for (original_chunk, decoded_chunk) in original_content
            .chunks
            .iter()
            .zip(decoded_content.chunks.iter())
        {
            assert_eq!(original_chunk.hash, decoded_chunk.hash);
        }

        // Verify the decoded content against the original data
        assert!(decoded_content.verify(&padded_data));
    }

    #[test]
    fn test_hash_store() {
        let data1 = b"Hello, world! This is a test.".to_vec();
        let data2 = b"Another test data for hash store.".to_vec();
        let chunk_size = 8;

        let padded_data1 = pad_to_chunk_size(&data1, chunk_size);
        let padded_data2 = pad_to_chunk_size(&data2, chunk_size);

        let content1 = Blob::from_bytes(&padded_data1, chunk_size);
        let content2 = Blob::from_bytes(&padded_data2, chunk_size);

        let mut hash_store = HashStore::default();

        content1.add_to_hash_store(&mut hash_store);
        assert_eq!(hash_store.chunks.len(), content1.chunks.len());

        content2.add_to_hash_store(&mut hash_store);
        let l1 = hash_store.chunks.len();

        // Add content1 again, should not increase the size of the store
        content1.add_to_hash_store(&mut hash_store);
        assert_eq!(hash_store.chunks.len(), l1);

        // Verify all chunks from both contents are in the store
        for chunk in content1.chunks.iter().chain(content2.chunks.iter()) {
            assert!(hash_store.contains(chunk));
        }
    }
}
