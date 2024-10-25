use std::{path::PathBuf, sync::Arc};

use bup::storage::Storage;
use clap::{Parser, Subcommand};
use object_store::local::LocalFileSystem;
use tracing::info;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long)]
    local_s3: PathBuf,
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
    },
    Restore {
        #[arg(long)]
        output: PathBuf,
    },
}

#[tokio::main]
#[allow(unreachable_code, unused_variables)]
pub async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt::init();
    let storage = LocalFileSystem::new_with_prefix(&cli.local_s3)?;
    let storage = Storage::new(Arc::new(storage), "root".into(), cli.local_data)?;

    match &cli.command {
        Commands::Backup { file } => {
            info!("Starting backup of file: {}", file.display());
            bup::backup(storage, file).await?;
            info!("Backup completed");
        }
        Commands::Restore { output } => {
            info!("Starting restore to: {}", output.display());
            bup::restore(storage, output).await?;
            info!("Restore completed");
        }
    }
    Ok(())
}
