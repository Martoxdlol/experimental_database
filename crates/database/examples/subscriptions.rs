use std::time::Duration;

use exdb::{Database, DatabaseConfig, SubscriptionMode, TransactionOptions, TransactionResult};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open_in_memory(DatabaseConfig::default(), None).await?;

    let mut schema = db.begin(TransactionOptions::default())?;
    schema.create_collection("users").await?;
    match schema.commit().await? {
        TransactionResult::Success { .. } => {}
        other => return Err(format!("schema transaction failed: {other:?}").into()),
    }

    let mut watch = db.begin(TransactionOptions {
        readonly: true,
        subscription: SubscriptionMode::Notify,
        session_id: 1,
    })?;
    let initial = watch
        .query("users", "_created_at", &[], None, None, None)
        .await?;
    println!("initial user count: {}", initial.len());
    let mut subscription = match watch.commit().await? {
        TransactionResult::Success {
            subscription_handle: Some(handle),
            ..
        } => handle,
        other => return Err(format!("subscription registration failed: {other:?}").into()),
    };

    let mut write = db.begin(TransactionOptions::default())?;
    write
        .insert("users", json!({"name": "Carol", "age": 28}))
        .await?;
    match write.commit().await? {
        TransactionResult::Success { commit_ts, .. } => {
            println!("committed invalidating write at ts {commit_ts}");
        }
        other => return Err(format!("write transaction failed: {other:?}").into()),
    }

    let event = tokio::time::timeout(Duration::from_secs(2), subscription.next_event())
        .await?
        .ok_or("subscription channel closed before invalidation")?;
    println!(
        "subscription invalidated by commit {} for queries {:?}",
        event.commit_ts, event.affected_query_ids
    );

    db.close().await?;
    Ok(())
}
