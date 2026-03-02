use std::net::SocketAddr;
use experimental_database::{Database, DbConfig};
use experimental_database::types::{ReplicationConfig, ReplicationRole};

/// Run the experimental-db server.
///
/// Usage:
///   experimental_database [data_dir] [ws_addr] [--primary <repl_addr>] [--replica <primary_addr>]
///
/// Examples:
///   # Standalone (default)
///   cargo run -- data 127.0.0.1:3000
///
///   # Primary with replication listener
///   cargo run -- data 127.0.0.1:3000 --primary 0.0.0.0:3001
///
///   # Replica connecting to primary
///   cargo run -- replica_data 127.0.0.1:3002 --replica 127.0.0.1:3001
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();

    let data_dir  = args.get(1).map(String::as_str).unwrap_or("data").to_string();
    let ws_addr: SocketAddr = args
        .get(2)
        .map(String::as_str)
        .unwrap_or("127.0.0.1:3000")
        .parse()?;

    // Parse optional replication flags
    let primary_repl_addr: Option<SocketAddr> = find_arg(&args, "--primary");
    let replica_primary_addr: Option<SocketAddr> = find_arg(&args, "--replica");

    let replication = if let Some(listen_addr) = primary_repl_addr {
        ReplicationConfig {
            role: ReplicationRole::Primary,
            listen_addr: Some(listen_addr.to_string()),
            primary_addr: None,
        }
    } else if let Some(primary_addr) = replica_primary_addr {
        ReplicationConfig {
            role: ReplicationRole::Replica,
            primary_addr: Some(primary_addr.to_string()),
            listen_addr: None,
        }
    } else {
        ReplicationConfig::default() // Standalone
    };

    let cfg = DbConfig::new(&data_dir).with_replication(replication.clone());
    let db = Database::open(cfg).await?;

    println!("Database opened at '{data_dir}'");
    println!("WebSocket API: ws://{ws_addr}/ws");

    // Start replication if configured
    match &replication.role {
        ReplicationRole::Primary => {
            if let Some(addr_str) = &replication.listen_addr {
                let repl_addr: SocketAddr = addr_str.parse()?;
                experimental_database::replication::start_primary_server(&db, repl_addr).await?;
                println!("Replication server: {repl_addr}");
            }
        }
        ReplicationRole::Replica => {
            if let Some(addr_str) = &replication.primary_addr {
                let primary_addr: SocketAddr = addr_str.parse()?;
                experimental_database::replication::start_replica_client(db.clone(), primary_addr).await?;
                println!("Replica: connected to primary at {primary_addr}");
            }
        }
        ReplicationRole::Standalone => {}
    }

    experimental_database::server::serve(db, ws_addr).await
}

fn find_arg<T: std::str::FromStr>(args: &[String], flag: &str) -> Option<T> {
    args.windows(2)
        .find(|w| w[0] == flag)
        .and_then(|w| w[1].parse().ok())
}
