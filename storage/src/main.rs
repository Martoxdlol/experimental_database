use std::path::Path;

use storage::engine::{DatabaseConfig, StorageEngine};

fn main() {
    println!("Storage engine");

    drop(StorageEngine::open(Path::new("./data"), DatabaseConfig::default()));
}
