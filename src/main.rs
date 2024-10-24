use std::{path::PathBuf, sync::Arc};

use clap::{Parser, Subcommand};
use object_store::memory::InMemory;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
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

    let storage = InMemory::new();
    match &cli.command {
        Commands::Backup { file } => {
            bup::backup(Arc::new(storage), file, "root".into()).await?;
            println!("Backup completed successfully.");
        }
        Commands::Restore { output } => {
            bup::restore(Arc::new(storage), output, "root".into()).await?;
            println!("Restore completed successfully.");
        }
    }

    Ok(())
}
