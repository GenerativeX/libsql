//! Focused restore test: drive `bottomless::Replicator::restore` directly
//! against the configured backend (Azure via LIBSQL_BOTTOMLESS_PROVIDER=azure),
//! isolating the storage layer from sqld's namespace/metastore orchestration.
//!
//! Env: the usual LIBSQL_BOTTOMLESS_* (PROVIDER, BUCKET, DATABASE_ID, creds).
//! RESTORE_DB_PATH: where to materialize the restored SQLite db.

use bottomless::replicator::{Options, Replicator};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    let options = Options::from_env()?;
    let db_path =
        std::env::var("RESTORE_DB_PATH").unwrap_or_else(|_| "/tmp/azrestore/data".to_string());
    if let Some(parent) = std::path::Path::new(&db_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let _ = std::fs::remove_file(&db_path);

    let mut replicator = Replicator::with_options(&db_path, options).await?;
    let (action, recovered) = replicator.restore(None, None).await?;
    println!("RESTORE_RESULT action={action:?} recovered={recovered}");
    Ok(())
}
