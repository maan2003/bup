use std::path::PathBuf;

use bup::storage::InMemoryStorage;
use clap::{Parser, Subcommand};

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
    CheckChanged {
        #[arg(short, long)]
        file: PathBuf,
    },
    PartialVerify {},
}

#[tokio::main]
#[allow(unreachable_code, unused_variables)]
pub async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // TODO: Initialize storage and config here
    let config: bup::BackupConfig<InMemoryStorage> = todo!();

    match &cli.command {
        Commands::Backup { file } => {
            bup::backup(config, file).await?;
            println!("Backup completed successfully.");
        }
        Commands::Restore { output } => {
            bup::restore(config, output).await?;
            println!("Restore completed successfully.");
        }
        Commands::CheckChanged { file } => {
            let is_valid = bup::check_changed(config, file).await?;
            if is_valid {
                println!("backup is up to date.");
            } else {
                println!("backup is stale.");
            }
        }
        Commands::PartialVerify {} => {
            if bup::partial_verify(config).await? {
                println!("Partial verification passed.");
            } else {
                println!("Partial verification failed.");
            }
        }
    }

    Ok(())
}
