use exdb::{Database, DatabaseConfig, TransactionOptions, TransactionResult};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let db = Database::open(temp.path(), DatabaseConfig::default(), None).await?;

    let mut schema = db.begin(TransactionOptions::default())?;
    schema.create_collection("users").await?;
    match schema.commit().await? {
        TransactionResult::Success { .. } => {}
        other => return Err(format!("schema transaction failed: {other:?}").into()),
    }

    let mut write = db.begin(TransactionOptions::default())?;
    write
        .insert("users", json!({"name": "Alice", "age": 30}))
        .await?;
    write
        .insert("users", json!({"name": "Bob", "age": 42}))
        .await?;
    match write.commit().await? {
        TransactionResult::Success { commit_ts, .. } => {
            println!("committed users at ts {commit_ts}");
        }
        other => return Err(format!("write transaction failed: {other:?}").into()),
    }

    let mut read = db.begin(TransactionOptions::readonly())?;
    let users = read
        .query("users", "_created_at", &[], None, None, None)
        .await?;
    read.rollback();

    for user in users {
        println!("{}", serde_json::to_string_pretty(&user)?);
    }

    db.close().await?;
    Ok(())
}
