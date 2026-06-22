# DBOS Transact

Lightweight **durable workflow orchestration** on Postgres or SQLite.

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

## Install

This is a Cargo workspace. Depend on the engine crate `dbos-core` (library name `dbos`):

```toml
[dependencies]
dbos-core = { git = "https://github.com/hwuiwon/dbos-transact-rust" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The optional `dbos-server` crate adds an admin HTTP API and a control-plane WebSocket client; the
`dbos-cli` crate provides a `dbos` binary.

## Concepts

### Context

Create a context, register your workflows and queues, then `launch`. Registration must happen before
launch; running/management happens after.

```rust
let ctx = dbos::new_context(Config {
    app_name: "myapp".into(),
    database_url: Some("postgres://localhost/myapp".into()),
    ..Default::default()
}).await?;
// register_workflow / register_queue / register_scheduled_workflow ...
ctx.launch().await?;
// run_workflow / enqueue / ... 
ctx.shutdown(Duration::from_secs(5)).await;
```

### Workflows and steps

A workflow is an `async fn(WfCtx, P) -> Result<R, DbosError>` registered under a name. Side effects
go in **steps** (`ctx.run_step`), which are checkpointed and replayed on recovery instead of re-run.

```rust
async fn my_workflow(ctx: WfCtx, input: MyInput) -> Result<MyOutput, DbosError> {
    let a = ctx.run_step("fetch", |_s| async move { fetch().await }).await?;
    let b = ctx.run_step("process", move |_s| async move { process(a).await }).await?;
    Ok(b)
}
dbos::register_workflow::<MyInput, MyOutput, _, _>(&ctx, "my_workflow", my_workflow)?;
```

Steps can retry with exponential backoff:

```rust
use dbos::StepOptions;
ctx.run_step_opts("call_api", StepOptions { max_retries: 5, ..Default::default() },
    |_s| async move { call_api().await }).await?;
```

Run options control the workflow id, queue, deduplication, priority, delay, timeout, and auth:

```rust
let handle = dbos::run_workflow::<P, R>(&ctx, "my_workflow", input, RunOptions {
    workflow_id: Some("idempotency-key".into()),
    timeout: Some(Duration::from_secs(30)),
    ..Default::default()
}).await?;
let result: R = handle.get_result().await?;
```

Re-running with the same `workflow_id` is idempotent — it attaches to the existing run.

### Child workflows, sleep

```rust
// inside a workflow body:
let child = ctx.run_child_workflow::<P, R>("other_workflow", input, RunOptions::default()).await?;
let r = child.get_result().await?;
ctx.sleep(Duration::from_secs(60)).await?;   // durable: a recovered workflow sleeps the remainder
```

### Queues

Queues bound concurrency and rate, and support priority and delayed execution:

```rust
use dbos::{QueueOptions, RateLimiter};
dbos::register_queue(&ctx, "work", QueueOptions {
    global_concurrency: Some(10),
    worker_concurrency: Some(2),
    rate_limit: Some(RateLimiter { limit: 100, period: Duration::from_secs(1) }),
    ..Default::default()
})?; // before launch

// enqueue:
let h = dbos::run_workflow::<P, R>(&ctx, "my_workflow", input,
    RunOptions { queue: Some("work".into()), ..Default::default() }).await?;
```

### Scheduling

Run a workflow on a cron schedule (5- or 6-field). The workflow receives the scheduled time:

```rust
use dbos::ScheduledTime;
async fn nightly(ctx: WfCtx, _scheduled: ScheduledTime) -> Result<(), DbosError> { Ok(()) }
dbos::register_scheduled_workflow::<(), _, _>(&ctx, "nightly", "0 0 * * *", nightly)?; // static

// or manage schedules at runtime:
dbos::create_schedule(&ctx, dbos::CreateScheduleOptions { /* name, workflow, cron, ... */ }).await?;
dbos::pause_schedule(&ctx, "nightly").await?;
dbos::resume_schedule(&ctx, "nightly").await?;
dbos::delete_schedule(&ctx, "nightly").await?;
```

### Notifications and events

```rust
// notifications (FIFO, exactly-once):
dbos::send(&ctx, "target-workflow-id", payload, "topic").await?;          // from anywhere
let msg: Msg = ctx.recv("topic", Duration::from_secs(30)).await?;          // inside a workflow

// key/value events:
ctx.set_event("status", "ready").await?;                                   // inside a workflow
let v: Option<String> = dbos::get_event(&ctx, "wf-id", "status", Duration::from_secs(5)).await?;

// append-only streams:
ctx.write_stream("log", entry).await?;
ctx.close_stream("log").await?;
let (entries, closed): (Vec<Entry>, bool) = dbos::read_stream(&ctx, "wf-id", "log").await?;
```

### Managing workflows

```rust
dbos::cancel_workflow(&ctx, "wf-id").await?;
let h = dbos::resume_workflow::<R>(&ctx, "wf-id").await?;
let h = dbos::fork_workflow::<R>(&ctx, dbos::ForkOptions { original_workflow_id: "wf-id".into(), start_step: 2, ..Default::default() }).await?;
let list = dbos::list_workflows(&ctx, dbos::ListWorkflowsInput::default()).await?;
let steps = dbos::get_workflow_steps(&ctx, "wf-id").await?;
```

### Debouncer

Coalesce a burst of calls for the same key into a single run after the input settles:

```rust
let deb = dbos::Debouncer::<In, Out>::new(&ctx, "my_workflow", Duration::from_secs(10))?; // before launch
let handle = deb.debounce(&ctx, "doc-123", Duration::from_millis(500), input).await?;
```

### External client

Connect from a separate process to enqueue and manage workflows without running an executor:

```rust
let client = dbos::Client::new(dbos::ClientConfig {
    app_name: "client".into(),
    database_url: "postgres://localhost/myapp".into(),
    ..Default::default()
}).await?;
let h = client.enqueue::<P, R>("work", "my_workflow", input, dbos::EnqueueOptions::default()).await?;
```

## Backends

SQLite and Postgres are both first-class, selected by the `database_url` scheme
(`sqlite::memory:`, `sqlite:///path/to.db`, or `postgres://…`). One portable migration set is shared
by both. Postgres connections automatically retry transient database failures, so workflows survive
a database restart.

## Admin server & CLI

```rust
let handle = dbos_server::start_admin_server(ctx.clone(), 3001).await?; // GET /dbos-healthz, POST /workflows, ...
```

```sh
dbos serve --port 3001 --database-url postgres://localhost/myapp
dbos workflow list --status PENDING
dbos workflow cancel <id>
```

## Testing

```sh
cargo test --workspace                                  # in-process SQLite, no services
DBOS_TEST_DATABASE_URL=postgres://localhost cargo test -p dbos-core -- --test-threads=4   # against Postgres
```

## License

MIT — see [LICENSE](LICENSE).
