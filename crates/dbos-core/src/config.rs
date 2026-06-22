//! Configuration and its validation/normalization.

use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use crate::constants::{DEFAULT_DATABASE_SCHEMA, DEFAULT_EXECUTOR_ID};
use crate::error::DbosError;
use crate::serialization::Serializer;

/// User-facing configuration for a DBOS context.
#[derive(Clone, Default)]
pub struct Config {
    /// Application name (REQUIRED).
    pub app_name: String,
    /// System database connection URL. Exactly one of `database_url` /
    /// `sqlite_pool` must be provided.
    pub database_url: Option<String>,
    /// Pre-built SQLite pool (alternative to `database_url`).
    pub sqlite_pool: Option<sqlx::SqlitePool>,
    /// System-database schema (Postgres only; ignored for SQLite). Default `dbos`.
    pub database_schema: Option<String>,
    /// Application version. Overridden by `DBOS__APPVERSION`; otherwise an
    /// executable-derived hash.
    pub application_version: Option<String>,
    /// Executor id. Overridden by `DBOS__VMID`; otherwise `local`.
    pub executor_id: Option<String>,
    /// Application id. Overridden by `DBOS__APPID`.
    pub application_id: Option<String>,
    /// Custom serializer (default JSON).
    pub serializer: Option<Arc<dyn Serializer>>,
    /// Whether to start the admin HTTP server on launch.
    pub admin_server: bool,
    /// Admin server port (default 3001).
    pub admin_server_port: Option<u16>,
    /// Whether patching is enabled.
    pub enable_patching: bool,
    /// Scheduler reconciler poll interval (default 30s).
    pub scheduler_polling_interval: Option<Duration>,
}

/// Validated, fully-resolved configuration.
#[derive(Clone)]
#[allow(dead_code)] // several fields are consumed by later-phase subsystems (scheduler, admin server)
pub(crate) struct ProcessedConfig {
    pub app_name: String,
    pub database_url: Option<String>,
    pub sqlite_pool: Option<sqlx::SqlitePool>,
    pub database_schema: String,
    pub application_version: String,
    pub executor_id: String,
    pub application_id: String,
    pub serializer: Option<Arc<dyn Serializer>>,
    pub admin_server: bool,
    pub admin_server_port: u16,
    pub enable_patching: bool,
    pub scheduler_polling_interval: Duration,
}

/// Validate and normalize a [`Config`], applying env overrides and defaults.
pub(crate) fn process_config(cfg: Config) -> Result<ProcessedConfig, DbosError> {
    if cfg.app_name.trim().is_empty() {
        return Err(DbosError::initialization("app name is required"));
    }
    let has_url = cfg.database_url.as_ref().is_some_and(|u| !u.is_empty());
    let has_pool = cfg.sqlite_pool.is_some();
    match (has_url, has_pool) {
        (false, false) => {
            return Err(DbosError::initialization(
                "a database connection is required (set database_url or sqlite_pool)",
            ));
        }
        (true, true) => {
            return Err(DbosError::initialization(
                "provide exactly one of database_url or sqlite_pool, not both",
            ));
        }
        _ => {}
    }

    let executor_id = env_override("DBOS__VMID")
        .or(cfg.executor_id.filter(|s| !s.is_empty()))
        .unwrap_or_else(|| DEFAULT_EXECUTOR_ID.to_string());

    let application_id =
        env_override("DBOS__APPID").or(cfg.application_id).unwrap_or_default();

    let application_version = env_override("DBOS__APPVERSION")
        .or(cfg.application_version.filter(|s| !s.is_empty()))
        .unwrap_or_else(computed_app_version);

    Ok(ProcessedConfig {
        app_name: cfg.app_name,
        database_url: cfg.database_url,
        sqlite_pool: cfg.sqlite_pool,
        database_schema: cfg
            .database_schema
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_DATABASE_SCHEMA.to_string()),
        application_version,
        executor_id,
        application_id,
        serializer: cfg.serializer,
        admin_server: cfg.admin_server,
        admin_server_port: cfg.admin_server_port.unwrap_or(3001),
        enable_patching: cfg.enable_patching,
        scheduler_polling_interval: cfg
            .scheduler_polling_interval
            .unwrap_or_else(|| Duration::from_secs(30)),
    })
}

fn env_override(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

/// Deterministic per-build application version derived from the executable's
/// path, length, and modified time (a stand-in for a binary content hash).
fn computed_app_version() -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    if let Ok(exe) = std::env::current_exe() {
        exe.hash(&mut hasher);
        if let Ok(meta) = std::fs::metadata(&exe) {
            meta.len().hash(&mut hasher);
            if let Ok(modified) = meta.modified() {
                if let Ok(dur) = modified.duration_since(std::time::UNIX_EPOCH) {
                    dur.as_secs().hash(&mut hasher);
                }
            }
        }
    }
    format!("{:016x}", hasher.finish())
}
