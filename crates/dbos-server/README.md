# dbos-server

Optional networking for [`dbos-core`](https://crates.io/crates/dbos-core): an
[axum](https://github.com/tokio-rs/axum) **admin HTTP API** (health, workflow list/get/steps,
cancel/resume/fork, recovery, garbage-collect, global-timeout) and a reconnecting **conductor**
WebSocket client for the DBOS control plane.

> A Rust port of [**DBOS Transact**](https://github.com/dbos-inc) by [DBOS, Inc.](https://www.dbos.dev)
> Independent, community implementation.

```rust,no_run
# async fn run(ctx: std::sync::Arc<dbos::DbosContext>) -> Result<(), dbos::DbosError> {
let handle = dbos_server::start_admin_server(ctx.clone(), 3001).await?;
// ... GET /dbos-healthz, POST /workflows, GET /workflows/{id}, ...
handle.shutdown().await;
# Ok(()) }
```

See the [project README](https://github.com/hwuiwon/dbos-transact-rust) for details.

## License

MIT.
