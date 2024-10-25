use std::{path::PathBuf, sync::Arc};

use bup::storage::Storage;
use clap::{Parser, Subcommand};
use object_store::local::LocalFileSystem;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    local_s3: PathBuf,
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    Backup {
        #[arg(short, long)]
        file: PathBuf,
    },
    Restore {
        #[arg(short, long)]
        output: PathBuf,
    },
}

#[tokio::main]
#[allow(unreachable_code, unused_variables)]
pub async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let storage = LocalFileSystem::new_with_prefix(&cli.local_s3)?;
    let storage = Storage::new(Arc::new(storage), "root".into());

    match &cli.command {
        Commands::Backup { file } => {
            bup::backup(storage, file).await?;
            println!("Backup completed successfully.");
        }
        Commands::Restore { output } => {
            bup::restore(storage, output).await?;
            println!("Restore completed successfully.");
        }
    }
    Ok(())
}
