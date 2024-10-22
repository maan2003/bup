use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[async_trait::async_trait]
pub trait Storage {
    async fn has(&self, key: &[u8]) -> anyhow::Result<bool>;
    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
    async fn get(&self, key: &[u8]) -> anyhow::Result<Vec<u8>>;
    async fn remove(&self, key: &[u8]) -> anyhow::Result<()>;
}
pub struct InMemoryStorage {
    data: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        InMemoryStorage {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
#[async_trait::async_trait]
impl Storage for InMemoryStorage {
    async fn has(&self, hash: &[u8]) -> anyhow::Result<bool> {
        Ok(self.data.lock().await.contains_key(hash))
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.data.lock().await.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.data
            .lock()
            .await
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Key not found"))
    }

    async fn remove(&self, key: &[u8]) -> anyhow::Result<()> {
        self.data.lock().await.remove(key);
        Ok(())
    }
}
