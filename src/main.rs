use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use indicatif::MultiProgress;
use std::fs::create_dir;

use diffed_places_pipeline::{build_coverage, import_atp, import_osm};

#[derive(Parser)]
#[command(name = "diffed-places-pipeline")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Run {
        #[arg(short, long, value_name = "alltheplaces.zip")]
        atp: PathBuf,

        #[arg(short, long, value_name = "workdir")]
        workdir: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Cli::parse();
    env_logger::init();
    match &args.command {
        Some(Commands::Run { atp, workdir }) => {
            let progress = MultiProgress::new();
            if !workdir.exists() {
                create_dir(workdir)?;
            }

            let atp = import_atp(atp, &progress, workdir)?;
            let coverage = build_coverage(&atp, &progress, workdir)?;
            import_osm(&coverage, &progress, workdir)?;
            Ok(())
        }
        None => Err(anyhow!("no subcommand given")),
    }
}
