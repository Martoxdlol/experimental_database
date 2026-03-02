use std::net::SocketAddr;
use experimental_database::{Database, DbConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let data_dir = std::env::args().nth(1).unwrap_or_else(|| "data".to_string());
    let addr: SocketAddr = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:3000".to_string())
        .parse()?;

    let db = Database::open(DbConfig::new(&data_dir)).await?;
    println!("Database opened at '{data_dir}'");
    println!("Connect via WebSocket: ws://{addr}/ws");

    experimental_database::server::serve(db, addr).await
}
