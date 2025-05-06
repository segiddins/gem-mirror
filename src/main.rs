#![feature(impl_trait_in_assoc_type)]

use std::{
    io::stdout,
    path::PathBuf,
};

use clap::Parser;
use miette::{Result, bail, miette};
use serde_json::json;
use storage::Store as _;

pub mod storage;

#[derive(Debug, clap::Parser)]
struct Command {
    /// The command to run
    #[clap(subcommand)]
    command: CommandType,

    /// The path to the store
    #[clap(long, default_value = "store")]
    store_path: PathBuf,
}

#[derive(Debug, clap::Subcommand)]
/// The command to run
enum CommandType {
    /// Update the store
    Update,
    /// Add a new index
    AddIndex { url: String },
    /// Print the path to each gem on a line
    EachGem {},
}

fn main() -> Result<()> {
    let command = Command::try_parse().map_err(|e| miette!(e))?;

    let mut store = storage::FsStore::new(&command.store_path)?;

    match command.command {
        CommandType::Update => {
            storage::update_store(store)?;
        }
        CommandType::AddIndex { url } => {
            store.add_index(url)?;
        }
        CommandType::EachGem {} => {
            for index in store.list_indices()? {
                for namespace in index.gems.values() {
                    for (_, gem) in namespace.versions.iter() {
                        if !gem.stored {
                            bail!("Gem {} is not stored", gem.full_name);
                        }
                        let gem_path = content_path(&command.store_path, &gem.package_integrity);
                        let json = json!({
                            "name": gem.name,
                            "version": gem.version,
                            "source": index.source,
                            "platform": gem.platform,
                            "full_name": gem.full_name,
                            "integrity": gem.package_integrity.to_string(),
                            "path": gem_path.display().to_string(),
                        });
                        serde_json::to_writer(stdout(), &json)
                            .map_err(|e| miette!("failed to serialize json: {}", e))?;
                        println!();
                    }
                }
            }
        }
    }

    Ok(())
}

fn content_path(cache: &PathBuf, sri: &ssri::Integrity) -> PathBuf {
    let mut path = PathBuf::new();
    let (algo, hex) = sri.to_hex();
    path.push(cache);
    path.push(format!("content-v{}", 2));
    path.push(algo.to_string());
    path.push(&hex[0..2]);
    path.push(&hex[2..4]);
    path.push(&hex[4..]);
    path
}
