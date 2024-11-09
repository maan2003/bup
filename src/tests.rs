use object_store::local::LocalFileSystem;
use rand::{thread_rng, RngCore};
use std::{
    fs,
    io::{ErrorKind, Read},
    os::unix::fs::FileExt,
    path::Path,
    sync::Arc,
};
use tempfile::tempdir;

use crate::Storage;

const BUFFER_SIZE: usize = 64 * 1024;

#[tokio::test]
async fn test_backup_and_restore() -> anyhow::Result<()> {
    // Create temporary directories for test
    let backup_dir = tempdir()?;
    let data_dir = tempdir()?;

    // Create a random test file
    let test_file_path = data_dir.path().join("test_file.bin");
    let file = fs::File::create(&test_file_path)?;
    write_random_data(file.try_clone()?, 0, 1024 * 1024 * 10).await?; // 10MB

    // Create output path for restore
    let restore_file_path = data_dir.path().join("restored_file.bin");

    // Initialize storage with test filesystem backend
    let storage = Storage::new(
        Arc::new(LocalFileSystem::new_with_prefix(backup_dir.path())?),
        "test-root",
        data_dir.path().join("local-data"),
    )?;

    // Perform backup
    crate::backup(storage.clone(), &test_file_path).await?;

    // Perform restore
    crate::restore(storage.clone(), &restore_file_path).await?;

    assert_files_same(&test_file_path, &restore_file_path).await?;

    // write 1M random data at 2M offset
    write_random_data(file.try_clone()?, 2 * 1024 * 1024, 1024 * 1024).await?;

    // Perform incremental backup
    crate::backup(storage.clone(), &test_file_path).await?;

    // Perform restore
    crate::restore(storage.clone(), &restore_file_path).await?;
    assert_files_same(&test_file_path, &restore_file_path).await?;

    Ok(())
}

async fn write_random_data(
    file: fs::File,
    offset: usize,
    size: usize,
) -> Result<(), anyhow::Error> {
    tokio::task::spawn_blocking(move || {
        let mut rng = thread_rng();
        let mut buffer = vec![0u8; BUFFER_SIZE]; // 64KB chunks
        for i in 0..size / BUFFER_SIZE {
            rng.fill_bytes(&mut buffer);
            file.write_all_at(&buffer, (offset + i * BUFFER_SIZE) as u64)?;
        }
        anyhow::Ok(())
    })
    .await??;
    Ok(())
}

async fn assert_files_same(
    test_file_path: &Path,
    restore_file_path: &Path,
) -> Result<(), anyhow::Error> {
    let test_file_path = test_file_path.to_owned();
    let restore_file_path = restore_file_path.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut original = fs::File::open(&test_file_path)?;
        let mut restored = fs::File::open(&restore_file_path)?;
        let mut buf1 = vec![0u8; BUFFER_SIZE];
        let mut buf2 = vec![0u8; BUFFER_SIZE];

        loop {
            let r1 = original.read_exact(&mut buf1);
            let r2 = restored.read_exact(&mut buf2);

            match (r1, r2) {
                (Err(e1), Err(e2))
                    if e1.kind() == ErrorKind::UnexpectedEof
                        && e2.kind() == ErrorKind::UnexpectedEof =>
                {
                    return anyhow::Ok(());
                }
                (r1, r2) => (r1?, r2?),
            };

            assert_eq!(&buf1, &buf2, "Restored file doesn't match original");
        }
    })
    .await??;
    Ok(())
}
