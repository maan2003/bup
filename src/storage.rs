use super::Blob;
use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};
use object_store::{path::Path, ObjectStore};
use std::{collections::HashSet, sync::Arc};

#[derive(Clone)]
pub struct Storage {
    store: Arc<dyn ObjectStore>,
    root_key: Path,
    data_path: std::path::PathBuf,
    data: Arc<std::sync::Mutex<LocalData>>,
}

impl Storage {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        root_key: Path,
        data_path: std::path::PathBuf,
    ) -> anyhow::Result<Self> {
        let data = if let Ok(bytes) = std::fs::read(&data_path) {
            bincode::decode_from_slice(&bytes, bincode::config::standard())?.0
        } else {
            LocalData {
                hashes_stored: HashSet::new(),
                metadata: None,
            }
        };
        Ok(Self {
            store,
            root_key,
            data_path,
            data: Arc::new(std::sync::Mutex::new(data)),
        })
    }

    pub(crate) fn object_path(key: &[u8]) -> Path {
        Path::from(BASE64_URL_SAFE_NO_PAD.encode(key))
    }

    fn save_local_data(&self) -> anyhow::Result<()> {
        let data = self.data.lock().unwrap();
        let encoded = bincode::encode_to_vec(&*data, bincode::config::standard())?;
        std::fs::write(&self.data_path, encoded)?;
        Ok(())
    }

    pub async fn put_block(&self, hash: &blake3::Hash, data: Vec<u8>) -> anyhow::Result<()> {
        if !self.has_block(hash).await {
            let path = Self::object_path(hash.as_bytes());
            tracing::info!("Putting block {}", hash);
            self.store.put(&path, data.into()).await?;
            self.data
                .lock()
                .unwrap()
                .hashes_stored
                .insert(*hash.as_bytes());
            self.save_local_data()?;
        }
        Ok(())
    }

    pub async fn has_block(&self, hash: &blake3::Hash) -> bool {
        if self
            .data
            .lock()
            .unwrap()
            .hashes_stored
            .contains(hash.as_bytes())
        {
            return true;
        }

        tracing::info!("Checking online for block {}", hash);
        let path = Self::object_path(hash.as_bytes());
        let exists = self.store.head(&path).await.is_ok();

        if exists {
            self.data
                .lock()
                .unwrap()
                .hashes_stored
                .insert(*hash.as_bytes());
            let _ = self.save_local_data();
        }

        exists
    }

    pub async fn get_block(&self, hash: &blake3::Hash) -> anyhow::Result<Vec<u8>> {
        let path = Self::object_path(hash.as_bytes());
        let bytes = self.store.get(&path).await?.bytes().await?;
        Ok(bytes.to_vec())
    }

    pub async fn get_root_metadata(&self) -> anyhow::Result<Blob> {
        let metadata = {
            let data = self.data.lock().unwrap();
            data.metadata.clone()
        };
        if let Some(metadata) = metadata {
            Ok(metadata)
        } else {
            let bytes = self.store.get(&self.root_key).await?.bytes().await?;
            let decoded: Blob = bincode::decode_from_slice(&bytes, bincode::config::standard())?.0;
            self.data.lock().unwrap().metadata = Some(decoded.clone());
            self.save_local_data()?;
            Ok(decoded)
        }
    }

    pub async fn put_root_metadata(&self, metadata: &Blob) -> anyhow::Result<()> {
        let bytes = bincode::encode_to_vec(metadata, bincode::config::standard())?;
        self.store.put(&self.root_key, bytes.into()).await?;
        self.data.lock().unwrap().metadata = Some(metadata.clone());
        self.save_local_data()?;
        Ok(())
    }
}

#[derive(Clone, bincode::Encode, bincode::Decode)]
struct LocalData {
    hashes_stored: HashSet<[u8; 32]>,
    metadata: Option<Blob>,
}
