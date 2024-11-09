use crate::blob::Document;

use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};
use futures::StreamExt;
use object_store::{path::Path, ObjectStore};
use std::sync::Arc;

#[derive(Clone)]
pub struct Storage {
    store: Arc<dyn ObjectStore>,
    root_key: Path,
    data_path: std::path::PathBuf,
}

const ROOT_KEY_PREFIX_BYTE: char = 'R';
const CHUNK_KEY_PREFIX_BYTE: char = 'C';
impl Storage {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        root_key: &str,
        data_path: std::path::PathBuf,
    ) -> anyhow::Result<Self> {
        let root_key = Path::from(format!("{ROOT_KEY_PREFIX_BYTE}{root_key}"));
        Ok(Self {
            store,
            root_key,
            data_path,
        })
    }

    fn chunk_path(key: &[u8]) -> Path {
        let mut s = String::with_capacity(key.len() * 4 / 3 + 1);
        s.push(CHUNK_KEY_PREFIX_BYTE);
        BASE64_URL_SAFE_NO_PAD.encode_string(key, &mut s);
        Path::from(s)
    }

    pub async fn put_chunk(&self, hash: &blake3::Hash, data: Vec<u8>) -> anyhow::Result<()> {
        let path = Self::chunk_path(hash.as_bytes());
        tracing::info!("Putting block {}", path);
        self.store.put(&path, data.into()).await?;
        Ok(())
    }

    pub async fn has_chunk(&self, hash: &blake3::Hash) -> bool {
        let path = Self::chunk_path(hash.as_bytes());
        self.store.head(&path).await.is_ok()
    }

    pub async fn get_chunk(&self, hash: &blake3::Hash) -> anyhow::Result<Vec<u8>> {
        let path = Self::chunk_path(hash.as_bytes());
        let bytes = self.store.get(&path).await?.bytes().await?;
        Ok(bytes.to_vec())
    }

    pub async fn get_root_metadata(&self) -> anyhow::Result<Option<Document>> {
        match self.store.get(&self.root_key).await {
            Ok(get_result) => {
                let bytes = get_result.bytes().await?;
                let decoded: Document =
                    bincode::decode_from_slice(&bytes, bincode::config::standard())?.0;
                Ok(Some(decoded))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn put_root_metadata(&self, document: Document) -> anyhow::Result<()> {
        let bytes = bincode::encode_to_vec(&document, bincode::config::standard())?;
        self.store.put(&self.root_key, bytes.into()).await?;
        Ok(())
    }

    pub async fn available_hashes(&self) -> anyhow::Result<Vec<blake3::Hash>> {
        let mut hashes = Vec::new();
        while let Some(meta) = self.store.list(None).next().await {
            let path: String = meta?.location.into();
            if path.starts_with(CHUNK_KEY_PREFIX_BYTE) {
                let bytes = BASE64_URL_SAFE_NO_PAD.decode(&path[1..])?;
                if let Ok(bytes) = bytes.try_into() {
                    hashes.push(blake3::Hash::from_bytes(bytes));
                }
            }
        }
        Ok(hashes)
    }
}
