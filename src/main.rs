use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

mod import_atp;
mod place;

use crate::import_atp::import_atp;

#[derive(Parser)]
#[command(name = "diffed-places")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    ImportAtp {
        #[arg(short, long, value_name = "alltheplaces.zip")]
        input: PathBuf,

        #[arg(short, long, value_name = "alltheplaces.parquet")]
        output: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Cli::parse();
    match &args.command {
        Some(Commands::ImportAtp { input, output }) => {
            import_atp(input, output)?;
        }
        None => {
            eprintln!("no subcommand given");
        }
    }
    Ok(())
}
