//! WebSocket integration tests — spins up a real axum server on a random port.

use std::net::SocketAddr;
use std::sync::Arc;

use experimental_database::{Database, DbConfig};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio_tungstenite::{connect_async, tungstenite::Message};

// ── Test helpers ──────────────────────────────────────────────────────────────

async fn start_server() -> (SocketAddr, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::open(DbConfig::new(dir.path())).await.unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let app = experimental_database::server::router(Arc::new(db));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, dir)
}

type WsTx = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;
type WsRx = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

async fn ws_connect(addr: SocketAddr) -> (WsTx, WsRx) {
    let (stream, _) = connect_async(format!("ws://{addr}/ws")).await.unwrap();
    stream.split()
}

/// Send a command and return the matching response (skips server-push messages).
async fn cmd(tx: &mut WsTx, rx: &mut WsRx, msg: Value) -> Value {
    tx.send(Message::Text(msg.to_string())).await.unwrap();
    let req_id = msg["req_id"].clone();
    loop {
        if let Some(Ok(Message::Text(text))) = rx.next().await {
            let v: Value = serde_json::from_str(&text).unwrap();
            if v["req_id"] == req_id {
                return v;
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_health_endpoint() {
    let (addr, _dir) = start_server().await;
    let body = reqwest::get(format!("http://{addr}/health"))
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(body["ok"], true);
}

#[tokio::test]
async fn test_ws_collection_crud() {
    let (addr, _dir) = start_server().await;
    let (mut tx, mut rx) = ws_connect(addr).await;

    let r = cmd(&mut tx, &mut rx, json!({"req_id":1,"type":"create_collection","name":"items"})).await;
    assert_eq!(r["ok"], true);

    // Duplicate collection → error
    let r = cmd(&mut tx, &mut rx, json!({"req_id":2,"type":"create_collection","name":"items"})).await;
    assert_eq!(r["ok"], false);

    let r = cmd(&mut tx, &mut rx, json!({"req_id":3,"type":"delete_collection","name":"items"})).await;
    assert_eq!(r["ok"], true);
}

#[tokio::test]
async fn test_ws_insert_get_delete() {
    let (addr, _dir) = start_server().await;
    let (mut tx, mut rx) = ws_connect(addr).await;

    cmd(&mut tx, &mut rx, json!({"req_id":1,"type":"create_collection","name":"docs"})).await;

    // Insert
    let r = cmd(&mut tx, &mut rx, json!({"req_id":2,"type":"begin_mutation"})).await;
    let mtx = r["tx"].as_str().unwrap().to_string();

    let r = cmd(&mut tx, &mut rx, json!({
        "req_id":3,"type":"insert","tx":mtx,"collection":"docs",
        "doc":{"title":"hello","score":42}
    })).await;
    assert_eq!(r["ok"], true);
    let doc_id = r["doc_id"].as_str().unwrap().to_string();
    assert_eq!(doc_id.len(), 32);

    let r = cmd(&mut tx, &mut rx, json!({"req_id":4,"type":"commit_mutation","tx":mtx})).await;
    assert_eq!(r["result"], "committed");

    // Read back
    let r = cmd(&mut tx, &mut rx, json!({"req_id":5,"type":"begin_query","subscribe":false})).await;
    let qtx = r["tx"].as_str().unwrap().to_string();

    let r = cmd(&mut tx, &mut rx, json!({
        "req_id":6,"type":"query_get","tx":qtx,"collection":"docs","doc_id":doc_id
    })).await;
    assert_eq!(r["ok"], true);
    assert_eq!(r["doc"]["title"], "hello");
    assert_eq!(r["doc"]["score"], 42);

    cmd(&mut tx, &mut rx, json!({"req_id":7,"type":"commit_query","tx":qtx})).await;

    // Patch
    let r = cmd(&mut tx, &mut rx, json!({"req_id":8,"type":"begin_mutation"})).await;
    let mtx = r["tx"].as_str().unwrap().to_string();
    cmd(&mut tx, &mut rx, json!({
        "req_id":9,"type":"patch","tx":mtx,"collection":"docs","doc_id":doc_id,
        "patch":{"score":100}
    })).await;
    cmd(&mut tx, &mut rx, json!({"req_id":10,"type":"commit_mutation","tx":mtx})).await;

    // Verify patch
    let r = cmd(&mut tx, &mut rx, json!({"req_id":11,"type":"begin_query","subscribe":false})).await;
    let qtx = r["tx"].as_str().unwrap().to_string();
    let r = cmd(&mut tx, &mut rx, json!({
        "req_id":12,"type":"query_get","tx":qtx,"collection":"docs","doc_id":doc_id
    })).await;
    assert_eq!(r["doc"]["score"], 100);
    assert_eq!(r["doc"]["title"], "hello"); // unchanged
    cmd(&mut tx, &mut rx, json!({"req_id":13,"type":"commit_query","tx":qtx})).await;

    // Delete
    let r = cmd(&mut tx, &mut rx, json!({"req_id":14,"type":"begin_mutation"})).await;
    let mtx = r["tx"].as_str().unwrap().to_string();
    cmd(&mut tx, &mut rx, json!({
        "req_id":15,"type":"delete_doc","tx":mtx,"collection":"docs","doc_id":doc_id
    })).await;
    cmd(&mut tx, &mut rx, json!({"req_id":16,"type":"commit_mutation","tx":mtx})).await;

    let r = cmd(&mut tx, &mut rx, json!({"req_id":17,"type":"begin_query","subscribe":false})).await;
    let qtx = r["tx"].as_str().unwrap().to_string();
    let r = cmd(&mut tx, &mut rx, json!({
        "req_id":18,"type":"query_get","tx":qtx,"collection":"docs","doc_id":doc_id
    })).await;
    assert_eq!(r["doc"], Value::Null); // deleted
    cmd(&mut tx, &mut rx, json!({"req_id":19,"type":"commit_query","tx":qtx})).await;
}

#[tokio::test]
async fn test_ws_find_with_filters() {
    let (addr, _dir) = start_server().await;
    let (mut tx, mut rx) = ws_connect(addr).await;

    cmd(&mut tx, &mut rx, json!({"req_id":1,"type":"create_collection","name":"products"})).await;

    let r = cmd(&mut tx, &mut rx, json!({"req_id":2,"type":"begin_mutation"})).await;
    let mtx = r["tx"].as_str().unwrap().to_string();

    for (name, price, active) in [
        ("Apple", 1, true),
        ("Banana", 3, true),
        ("Cherry", 10, false),
        ("Date", 15, true),
    ] {
        cmd(&mut tx, &mut rx, json!({
            "req_id":3,"type":"insert","tx":mtx,"collection":"products",
            "doc":{"name":name,"price":price,"active":active}
        })).await;
    }
    cmd(&mut tx, &mut rx, json!({"req_id":4,"type":"commit_mutation","tx":mtx})).await;

    let r = cmd(&mut tx, &mut rx, json!({"req_id":5,"type":"begin_query","subscribe":false})).await;
    let qtx = r["tx"].as_str().unwrap().to_string();

    // price > 2
    let r = cmd(&mut tx, &mut rx, json!({
        "req_id":6,"type":"query_find","tx":qtx,"collection":"products",
        "filter":{"gt":["price",2]}
    })).await;
    assert_eq!(r["docs"].as_array().unwrap().len(), 3);

    // active=true AND price >= 3
    let r = cmd(&mut tx, &mut rx, json!({
        "req_id":7,"type":"query_find","tx":qtx,"collection":"products",
        "filter":{"and":[{"eq":["active",true]},{"gte":["price",3]}]}
    })).await;
    assert_eq!(r["docs"].as_array().unwrap().len(), 2); // Banana, Date

    // price IN [1, 10]
    let r = cmd(&mut tx, &mut rx, json!({
        "req_id":8,"type":"query_find_ids","tx":qtx,"collection":"products",
        "filter":{"in":["price",[1,10]]}
    })).await;
    assert_eq!(r["doc_ids"].as_array().unwrap().len(), 2); // Apple, Cherry

    cmd(&mut tx, &mut rx, json!({"req_id":9,"type":"commit_query","tx":qtx})).await;
}

#[tokio::test]
async fn test_ws_subscription_push() {
    let (addr, _dir) = start_server().await;
    let (mut tx, mut rx) = ws_connect(addr).await;

    cmd(&mut tx, &mut rx, json!({"req_id":1,"type":"create_collection","name":"events"})).await;

    // Subscribe via query session
    let r = cmd(&mut tx, &mut rx, json!({"req_id":2,"type":"begin_query","subscribe":true})).await;
    let qtx = r["tx"].as_str().unwrap().to_string();

    cmd(&mut tx, &mut rx, json!({
        "req_id":3,"type":"query_find","tx":qtx,"collection":"events","filter":null
    })).await;

    let r = cmd(&mut tx, &mut rx, json!({"req_id":4,"type":"commit_query","tx":qtx})).await;
    assert_eq!(r["subscribed"], true);
    let sub_id = r["sub_id"].as_u64().unwrap();

    // Write from a second connection to trigger invalidation
    let (mut tx2, mut rx2) = ws_connect(addr).await;
    let r = cmd(&mut tx2, &mut rx2, json!({"req_id":1,"type":"begin_mutation"})).await;
    let mtx2 = r["tx"].as_str().unwrap().to_string();
    cmd(&mut tx2, &mut rx2, json!({
        "req_id":2,"type":"insert","tx":mtx2,"collection":"events","doc":{"e":"ping"}
    })).await;
    cmd(&mut tx2, &mut rx2, json!({"req_id":3,"type":"commit_mutation","tx":mtx2})).await;

    // Expect the push event on the first connection
    let invalidation = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        async {
            loop {
                if let Some(Ok(Message::Text(text))) = rx.next().await {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["type"] == "invalidation" {
                        return v;
                    }
                }
            }
        },
    )
    .await
    .expect("timed out waiting for invalidation push");

    assert_eq!(invalidation["sub_id"], sub_id);
    assert!(invalidation["ts"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn test_ws_conflict_detection() {
    let (addr, _dir) = start_server().await;
    let (mut tx1, mut rx1) = ws_connect(addr).await;
    let (mut tx2, mut rx2) = ws_connect(addr).await;

    cmd(&mut tx1, &mut rx1, json!({"req_id":1,"type":"create_collection","name":"shared"})).await;

    // Insert initial doc
    let r = cmd(&mut tx1, &mut rx1, json!({"req_id":2,"type":"begin_mutation"})).await;
    let m = r["tx"].as_str().unwrap().to_string();
    let r = cmd(&mut tx1, &mut rx1, json!({
        "req_id":3,"type":"insert","tx":m,"collection":"shared","doc":{"v":1}
    })).await;
    let doc_id = r["doc_id"].as_str().unwrap().to_string();
    cmd(&mut tx1, &mut rx1, json!({"req_id":4,"type":"commit_mutation","tx":m})).await;

    // Both connections start mutations and read the same doc
    let r = cmd(&mut tx1, &mut rx1, json!({"req_id":5,"type":"begin_mutation"})).await;
    let m1 = r["tx"].as_str().unwrap().to_string();
    let r = cmd(&mut tx2, &mut rx2, json!({"req_id":1,"type":"begin_mutation"})).await;
    let m2 = r["tx"].as_str().unwrap().to_string();

    cmd(&mut tx1, &mut rx1, json!({
        "req_id":6,"type":"tx_get","tx":m1,"collection":"shared","doc_id":doc_id
    })).await;
    cmd(&mut tx2, &mut rx2, json!({
        "req_id":2,"type":"tx_get","tx":m2,"collection":"shared","doc_id":doc_id
    })).await;

    cmd(&mut tx1, &mut rx1, json!({
        "req_id":7,"type":"patch","tx":m1,"collection":"shared","doc_id":doc_id,"patch":{"v":2}
    })).await;
    cmd(&mut tx2, &mut rx2, json!({
        "req_id":3,"type":"patch","tx":m2,"collection":"shared","doc_id":doc_id,"patch":{"v":3}
    })).await;

    let r1 = cmd(&mut tx1, &mut rx1, json!({"req_id":8,"type":"commit_mutation","tx":m1})).await;
    assert_eq!(r1["result"], "committed");

    let r2 = cmd(&mut tx2, &mut rx2, json!({"req_id":4,"type":"commit_mutation","tx":m2})).await;
    assert_eq!(r2["result"], "conflict");
}

#[tokio::test]
async fn test_ws_error_unknown_command() {
    let (addr, _dir) = start_server().await;
    let (mut tx, mut rx) = ws_connect(addr).await;

    let r = cmd(&mut tx, &mut rx, json!({"req_id":1,"type":"does_not_exist"})).await;
    assert_eq!(r["ok"], false);
    assert!(r["error"].as_str().unwrap().contains("unknown command"));
}

#[tokio::test]
async fn test_ws_invalid_json() {
    let (addr, _dir) = start_server().await;
    let (mut tx, mut rx) = ws_connect(addr).await;

    tx.send(Message::Text("{not json}".to_string())).await.unwrap();
    if let Some(Ok(Message::Text(text))) = rx.next().await {
        let v: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(v["ok"], false);
    }
}
