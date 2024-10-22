pub struct FileChunk {
    hash: blake3::Hash,
}

pub struct FileContent {
    chunk_size: usize,
    chunks: Vec<FileChunk>,
}

impl FileContent {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_bytes_and_verify() {
        let data = b"Hello, world! This is a test.".to_vec();
        let chunk_size = 8;
        let padded_data = pad_to_chunk_size(&data, chunk_size);

        let file_content = FileContent::from_bytes(&padded_data, chunk_size);

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
        let file_content = FileContent::from_bytes(&data, chunk_size);

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
}
