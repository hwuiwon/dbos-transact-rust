# Architecture

DBOS Transact makes ordinary async Rust functions **durable**: a workflow's input, its output, and
each memoized *step* are checkpointed in a system database, so after a crash the workflow resumes
from its last completed step with exactly-once side-effect semantics.

## Crates

- **`dbos-core`** (library name `dbos`) — the engine: context lifecycle, the workflow/step
  execution model, the system database, queues, scheduler, debouncer, and client.
- **`dbos-server`** — an [axum](https://github.com/tokio-rs/axum) admin HTTP API and a reconnecting
  WebSocket conductor client (both optional networking layers).
- **`dbos-cli`** — the `dbos` binary (`serve`, `workflow list/get/steps/cancel/resume`).

## The context

`Arc<DbosContext>` is the cheaply-cloneable handle that owns everything: the database pool, the
in-memory workflow and queue registries, a `CancellationToken` (the cancellation root), a
`TaskTracker` (which all long-lived tasks join so shutdown can drain them), the queue runner, and
the scheduler. Free functions and `WfCtx`/`StepCtx` methods take `&Arc<DbosContext>`.

Lifecycle: `new_context(config)` builds it (no side effects beyond opening the pool);
`launch()` runs migrations, registers the application version, starts the queue runner and
schedule reconciler, and performs one recovery pass; `shutdown(timeout)` cancels the token, drains
in-flight work, and closes the pool. Workflows and queues are registered **before** launch.

## Durability model

All durable state lives behind an object-safe `SystemDatabase` trait, held as
`Arc<dyn SystemDatabase>`. There are two implementations — `SqliteDb` and `PostgresDb` — sharing
**one dialect-portable migration** (`crates/dbos-core/migrations/0001_initial_schema.sql`) applied
by a single-counter runner. Postgres connections are wrapped in a `RetryingDb` decorator that
retries transient connection failures with capped backoff, so workflows survive a database that
goes away and comes back.

Core tables:

- `workflow_status` — one row per workflow: status, name, input/output/error, executor, attempts,
  queue/dedup/priority/delay fields, timeout/deadline, and an `owner_xid` used for ownership.
- `operation_outputs` — one row per recorded step, keyed by `(workflow_uuid, function_id)`. This
  primary key is the exactly-once guarantee: a duplicate step record is a conflict.
- `notifications`, `workflow_events`(`_history`), `streams` — communication primitives.
- `queues`, `workflow_schedules`, `application_versions` — queue/schedule/version metadata.

Two durability invariants underpin everything:

1. **Deterministic step ids** — a per-run counter (starting at -1, pre-incremented) assigns each
   step a stable `function_id`, so a replay maps each call back to its recorded result.
2. **Memoization** — before running a step, the engine checks `operation_outputs`; a recorded
   result (success *or* error) is returned without re-executing. A function-name mismatch at a step
   id is reported as a non-determinism error.

## Typed erasure & serialization

Generic workflow inputs `P` and outputs `R` are serialized with `serde` at registration: each
registered workflow is stored as an erased closure
`Arc<dyn Fn(WfCtx, Option<String>, String) -> BoxFuture<Result<Option<String>, DbosError>>>` that
decodes `P`, runs the body, and encodes `R`. The stored form is `Option<String>` (`None` = SQL
NULL); every row records its serialization format (`DBOS_JSON` base64 by default, or `portable_json`
for cross-language interop) so the right decoder is chosen on read.

## Execution & recovery

`run_workflow` derives the workflow id, inserts the status row (handling attempt counting,
dead-letter parking, dedup, and the owner/skip decision), then either spawns the body on the task
tracker (returning a channel-backed handle) or — when the workflow is enqueued, already terminal, or
owned elsewhere — returns a polling handle. The body runs under `catch_unwind`, so a panic becomes a
recorded error; durable writes never run inside a cancellation branch.

Recovery lists `PENDING` workflows for an executor and, for each, either resets a queued workflow to
`ENQUEUED` (for the runner to re-dispatch) or re-runs the body via its erased entry — replaying
completed steps and incrementing the recovery-attempt counter, parking at the dead-letter state once
the maximum is exceeded.

## Concurrency

Everything is built on Tokio. The cancellation root is one `CancellationToken` (children derive via
`child_token()`); the `TaskTracker` is the join point for graceful shutdown. The queue runner is a
single supervisor loop that promotes delayed workflows, then dequeues and dispatches eligible work
per queue honoring global/worker concurrency, rate limits, and priority. The scheduler runs a
firing task per cron entry plus a reconciler that syncs the in-memory entries with the
`workflow_schedules` table. Notifications/events are awaited by polling durable state with a
checkpointed deadline, so a crash mid-wait resumes correctly.

## Testing

Every test runs against in-process SQLite with no external services (`cargo test --workspace`). The
same engine suite also runs against Postgres when `DBOS_TEST_DATABASE_URL` is set (each test
provisions its own database). A gated chaos test restarts a Postgres container while workflows are
in flight and asserts they all still complete correctly. Recovery is exercised deterministically
throughout the suite by forcing a completed workflow back to `PENDING` and recovering it.
