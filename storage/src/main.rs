use std::path::Path;

use storage::engine::{DatabaseConfig, StorageEngine};

fn main() {
    println!("Storage engine");

    let _ = StorageEngine::open(Path::new("./data"), DatabaseConfig::default());
}
