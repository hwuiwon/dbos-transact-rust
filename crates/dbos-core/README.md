# dbos-core

Durable workflow orchestration for Rust on Postgres or SQLite. The library name is `dbos`.

> A Rust port of [**DBOS Transact**](https://github.com/dbos-inc) — the durable-workflow platform by
> [DBOS, Inc.](https://www.dbos.dev) Independent, community implementation; see [Credits](#credits).

DBOS makes ordinary `async` functions **durable**: their inputs, outputs, and each *step* are
checkpointed in a database, so if your program crashes, each workflow automatically resumes from its
last completed step — with exactly-once side effects — when it restarts. You get durable workflows,
queues, scheduling, and notifications by writing normal Rust, with no separate orchestrator to run.

```rust
use std::time::Duration;
use dbos::{Config, WfCtx, DbosError, RunOptions};

async fn checkout(ctx: WfCtx, order_id: String) -> Result<String, DbosError> {
    // Each step is checkpointed: on recovery it is replayed, never re-run.
    let charge = ctx.run_step("charge_card", {
        let order_id = order_id.clone();
        move |_step| async move { charge_card(&order_id).await }
    }).await?;
    ctx.run_step("ship", move |_step| async move { ship(&order_id, &charge).await }).await
}

#[tokio::main]
async fn main() -> Result<(), DbosError> {
    let ctx = dbos::new_context(Config {
        app_name: "store".into(),
        database_url: Some("sqlite::memory:".into()),   // or "postgres://user:pass@host/db"
        ..Default::default()
    }).await?;

    dbos::register_workflow::<String, String, _, _>(&ctx, "checkout", checkout)?;
    ctx.launch().await?;

    let handle = dbos::run_workflow::<String, String>(
        &ctx, "checkout", "order-42".into(), RunOptions::default()).await?;
    println!("{}", handle.get_result().await?);

    ctx.shutdown(Duration::from_secs(5)).await;
    Ok(())
}
# async fn charge_card(_: &str) -> Result<String, DbosError> { Ok("ok".into()) }
# async fn ship(_: &str, _: &str) -> Result<String, DbosError> { Ok("shipped".into()) }
```

## Features

Durable workflows with automatic crash recovery; memoized steps with retry; child workflows; durable
sleep and workflow timeouts; durable queues (concurrency, rate limiting, priority, delayed
execution); cron scheduling (static and DB-backed dynamic) and a debouncer; notifications
(`send`/`recv`), events (`set_event`/`get_event`), and append-only streams; cancel/resume/fork/delete
and an external client. SQLite and Postgres are both first-class behind one trait; Postgres
connections transparently retry transient failures.

See the [project README](https://github.com/hwuiwon/dbos-transact-rust) for full usage and the
[architecture overview](https://github.com/hwuiwon/dbos-transact-rust/blob/main/docs/ARCHITECTURE.md).

## Credits

A Rust port of [DBOS Transact](https://github.com/dbos-inc) by [DBOS, Inc.](https://www.dbos.dev)
The programming model and reference implementations (Go/Python/TypeScript) are their work. This is
an independent Rust implementation, not affiliated with or endorsed by DBOS, Inc.

## License

MIT.
