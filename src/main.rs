use std::{path::PathBuf, sync::Arc};

use anyhow::Context;
use bup::storage::Storage;
use clap::{Args, Parser, Subcommand};
use object_store::{aws::AmazonS3Builder, local::LocalFileSystem};
use tracing::info;

#[derive(Args)]
#[group(required = true, multiple = false)]
struct BackendOpts {
    #[arg(long)]
    test_fs_backend: Option<PathBuf>,
    #[arg(long)]
    s3: bool,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(flatten)]
    backend: BackendOpts,
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    Backup {
        #[arg(long)]
        file: PathBuf,
    },
    Restore {
        #[arg(long)]
        output: PathBuf,
    },
    Info {},
}

#[tokio::main]
#[allow(unreachable_code, unused_variables)]
pub async fn main() -> anyhow::Result<()> {
    let cli: Cli = Cli::parse();
    tracing_subscriber::fmt::init();

    let storage = match cli.backend {
        BackendOpts {
            test_fs_backend: Some(path),
            s3: false,
        } => {
            let storage = LocalFileSystem::new_with_prefix(&path)?;
            Storage::new(Arc::new(storage))?
        }
        BackendOpts {
            test_fs_backend: None,
            s3: true,
        } => {
            let storage = AmazonS3Builder::from_env().build()?;
            Storage::new(Arc::new(storage))?
        }
        _ => unreachable!("Backend options are mutually exclusive"),
    };

    match cli.command {
        Commands::Backup { file } => {
            info!("Starting backup of file: {}", file.display());
            bup::backup(storage, &file).await?;
            info!("Backup completed");
        }
        Commands::Restore { output } => {
            info!("Starting restore to: {}", output.display());
            bup::restore(storage, &output).await?;
            info!("Restore completed");
        }
        Commands::Info {} => {
            info!("Getting version history");
            let metadata = storage
                .get_root_metadata()
                .await?
                .context("root is not present")?;

            let current = metadata.current();
            println!(
                "Size: {}",
                humansize::format_size(current.size(), humansize::BINARY)
            );
            println!("Last updated: {}", current.timestamp());
            for version in metadata.versions() {
                println!(
                    "Old Version from: {}, retained size: {}",
                    version.timestamp(),
                    humansize::format_size(version.retained_size(), humansize::BINARY),
                );
            }
        }
    }
    Ok(())
}
