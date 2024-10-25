use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};
use object_store::{path::Path, ObjectStore};
use std::sync::Arc;

#[derive(Clone)]
pub struct Storage {
    store: Arc<dyn ObjectStore>,
    root_key: Path,
}

impl Storage {
    pub fn new(store: Arc<dyn ObjectStore>, root_key: Path) -> Self {
        Self { store, root_key }
    }

    pub(crate) fn object_path(key: &[u8]) -> Path {
        Path::from(BASE64_URL_SAFE_NO_PAD.encode(key))
    }

    pub async fn put_block(&self, hash: &blake3::Hash, data: Vec<u8>) -> anyhow::Result<()> {
        if !self.has_block(hash).await {
            let path = Self::object_path(hash.as_bytes());
            self.store.put(&path, data.into()).await?;
        }
        Ok(())
    }

    pub async fn has_block(&self, hash: &blake3::Hash) -> bool {
        let path = Self::object_path(hash.as_bytes());
        self.store.head(&path).await.is_ok()
    }

    pub async fn get_block(&self, hash: &blake3::Hash) -> anyhow::Result<Vec<u8>> {
        let path = Self::object_path(hash.as_bytes());
        let bytes = self.store.get(&path).await?.bytes().await?;
        Ok(bytes.to_vec())
    }

    pub async fn get_root_metadata<T: bincode::Decode>(&self) -> anyhow::Result<T> {
        let bytes = self.store.get(&self.root_key).await?.bytes().await?;
        let decoded = bincode::decode_from_slice(&bytes, bincode::config::standard())?.0;
        Ok(decoded)
    }

    pub async fn put_root_metadata<T: bincode::Encode>(&self, metadata: &T) -> anyhow::Result<()> {
        let bytes = bincode::encode_to_vec(metadata, bincode::config::standard())?;
        self.store.put(&self.root_key, bytes.into()).await?;
        Ok(())
    }
}
