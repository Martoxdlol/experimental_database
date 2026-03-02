//! WAL-streaming replication: single primary, multiple read replicas.
//!
//! ## Primary
//! When `DbConfig.replication.role == Primary`, the database maintains an
//! internal `broadcast::Sender<Arc<Vec<u8>>>` that emits raw WAL frame
//! batches after each successful disk write. Call `start_primary_server` to
//! accept replica TCP connections and stream those frames to them.
//!
//! ## Replica
//! When `DbConfig.replication.role == Replica`, call `start_replica_client`
//! after opening the database. The task connects to the primary TCP address,
//! reads WAL frames, decodes them, and applies them to the local database.
//! The database is in read-only mode: mutations are rejected.
//!
//! ## Wire format
//! The TCP stream is a continuous sequence of raw WAL frames.
//! Each frame has the same layout as the WAL file:
//!   `len(u32 LE) | crc32(u32 LE) | record_type(u8) | payload(…)`
//! No additional framing is needed.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::storage::wal::WalRecord;
use crate::Database;

// ── Primary server ─────────────────────────────────────────────────────────────

/// Bind a TCP server that streams WAL frames to connecting replicas.
///
/// Must be called after `Database::open` with `role: Primary`.
/// The function spawns background tasks and returns immediately.
pub async fn start_primary_server(db: &Database, addr: SocketAddr) -> Result<()> {
    let mut rx = db
        .replication_subscribe()
        .ok_or_else(|| anyhow!("database is not configured as a primary"))?;

    // We need a broadcast channel that we can subscribe new connections to.
    // Re-use the DB's broadcast sender by subscribing a local mirror that
    // forwards to per-connection senders.
    let (fwd_tx, _) = tokio::sync::broadcast::channel::<Arc<Vec<u8>>>(1024);
    let fwd_tx2 = fwd_tx.clone();

    // Task: forward primary DB broadcasts → our local fwd channel
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(frame) => {
                    // Best-effort: ignore if no receivers
                    let _ = fwd_tx2.send(frame);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("replication broadcast lagged by {n} frames");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let listener = TcpListener::bind(addr).await?;
    tracing::info!("[replication] primary listening on {addr}");

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((socket, peer)) => {
                    tracing::info!("[replication] replica connected from {peer}");
                    let sub = fwd_tx.subscribe();
                    tokio::spawn(serve_replica(socket, sub));
                }
                Err(e) => {
                    tracing::error!("[replication] accept error: {e}");
                }
            }
        }
    });

    Ok(())
}

/// Serve one replica connection: stream WAL frames as they are broadcast.
async fn serve_replica(
    mut socket: TcpStream,
    mut rx: tokio::sync::broadcast::Receiver<Arc<Vec<u8>>>,
) {
    loop {
        match rx.recv().await {
            Ok(frame) => {
                if socket.write_all(&frame).await.is_err() {
                    break; // replica disconnected
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!("[replication] replica stream lagged by {n} frames; closing");
                break;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

// ── Replica client ─────────────────────────────────────────────────────────────

/// Connect to the primary and continuously apply WAL frames to `db`.
///
/// Must be called after `Database::open`. The function spawns a background
/// task and returns immediately.
pub async fn start_replica_client(db: Database, primary_addr: SocketAddr) -> Result<()> {
    tracing::info!("[replication] replica connecting to primary at {primary_addr}");

    let stream = TcpStream::connect(primary_addr).await?;
    tracing::info!("[replication] connected to primary");

    tokio::spawn(async move {
        if let Err(e) = run_replica(stream, db).await {
            tracing::error!("[replication] replica client error: {e}");
        }
    });

    Ok(())
}

/// Read WAL frames from the TCP stream and apply them to the replica database.
async fn run_replica(mut stream: TcpStream, db: Database) -> Result<()> {
    // In-flight transactions: tx_id → vec of records so far
    let mut in_flight: HashMap<u64, Vec<WalRecord>> = HashMap::new();

    loop {
        // Read frame header: len(u32 LE) + crc32(u32 LE)
        let mut header = [0u8; 8];
        match stream.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                tracing::info!("[replication] primary closed connection");
                break;
            }
            Err(e) => return Err(e.into()),
        }

        let frame_len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let expected_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());

        // Read frame body
        let mut body = vec![0u8; frame_len];
        stream.read_exact(&mut body).await?;

        // Verify CRC
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&body);
        let actual_crc = hasher.finalize();
        if actual_crc != expected_crc {
            return Err(anyhow!(
                "WAL frame CRC mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
            ));
        }

        // Decode and apply
        let record = WalRecord::decode(&body)?;
        apply_record(&db, &mut in_flight, record)?;
    }

    Ok(())
}

fn apply_record(
    db: &Database,
    in_flight: &mut HashMap<u64, Vec<WalRecord>>,
    record: WalRecord,
) -> Result<()> {
    match &record {
        WalRecord::Begin { tx_id, .. } => {
            in_flight.insert(*tx_id, vec![]);
        }
        WalRecord::PutDoc { tx_id, .. } | WalRecord::DeleteDoc { tx_id, .. } => {
            if let Some(ops) = in_flight.get_mut(tx_id) {
                ops.push(record);
            }
        }
        WalRecord::Commit { tx_id, commit_ts } => {
            if let Some(ops) = in_flight.remove(tx_id) {
                let commit_ts = *commit_ts;
                for op in &ops {
                    db.apply_wal_record(op, commit_ts)?;
                }
                // Advance applied_ts via the Commit record itself
                db.apply_wal_record(&record, commit_ts)?;
            }
        }
        WalRecord::Abort { tx_id } => {
            in_flight.remove(tx_id);
        }
        WalRecord::CatalogUpdate(_) => {
            db.apply_wal_record(&record, 0)?;
        }
        WalRecord::Checkpoint { .. } => {}
    }
    Ok(())
}
