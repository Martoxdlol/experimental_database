use std::error::Error;

mod vibe_db;

fn main() -> Result<(), Box<dyn Error>> {
    let dir = std::env::temp_dir().join("mini_page_db_example");
    std::fs::create_dir_all(&dir)?;

    let data_path = dir.join("data.db");
    let wal_path = dir.join("wal.log");

    let mut db = vibe_db::Db::open(&data_path, &wal_path, 128)?;

    // Write into page 0, payload offset 16 (start of payload).
    db.write_at(0, 16, b"hello")?;

    let bytes = db.read_at(0, 16, 5)?;
    assert_eq!(&bytes, b"hello");

    db.flush_all()?;
    Ok(())
}
