use std::{path::PathBuf, sync::Arc};

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
    #[arg(long)]
    local_data: PathBuf,
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    Backup {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        initial: bool,
    },
    Restore {
        #[arg(long)]
        output: PathBuf,
    },
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
            Storage::new(Arc::new(storage), "root".into(), cli.local_data)?
        }
        BackendOpts {
            test_fs_backend: None,
            s3: true,
        } => {
            let storage = AmazonS3Builder::from_env().build()?;
            Storage::new(Arc::new(storage), "root".into(), cli.local_data)?
        }
        _ => unreachable!("Backend options are mutually exclusive"),
    };

    match cli.command {
        Commands::Backup { file, initial } => {
            info!("Starting backup of file: {}", file.display());
            bup::backup(storage, &file, initial).await?;
            info!("Backup completed");
        }
        Commands::Restore { output } => {
            info!("Starting restore to: {}", output.display());
            bup::restore(storage, &output).await?;
            info!("Restore completed");
        }
    }
    Ok(())
}
