-- DBOS Transact (Rust) — consolidated, dialect-portable schema.
--
-- A single migration shared by both the SQLite and Postgres backends. It is the
-- final-state schema of upstream dbos-transact (all 40 upstream migrations
-- collapsed), written in SQL that BOTH engines accept:
--   * types: TEXT / INTEGER / BIGINT / BOOLEAN / NUMERIC / REAL (SQLite gives
--     BIGINT integer affinity; Postgres treats them natively).
--   * boolean literals TRUE/FALSE (SQLite >= 3.23, Postgres native).
--   * partial indexes (WHERE ...) — supported by both.
--   * no schema-qualified names (Postgres uses search_path), no CONCURRENTLY,
--     no driver-side DEFAULT functions, no plpgsql/triggers (the engine binds
--     all timestamps/uuids itself and polls instead of LISTEN/NOTIFY).

CREATE TABLE workflow_status (
    workflow_uuid TEXT PRIMARY KEY,
    status TEXT,
    name TEXT,
    authenticated_user TEXT,
    assumed_role TEXT,
    authenticated_roles TEXT,
    request TEXT,
    output TEXT,
    error TEXT,
    executor_id TEXT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    application_version TEXT,
    application_id TEXT,
    class_name TEXT DEFAULT NULL,
    config_name TEXT DEFAULT NULL,
    recovery_attempts BIGINT DEFAULT 0,
    queue_name TEXT,
    workflow_timeout_ms BIGINT,
    workflow_deadline_epoch_ms BIGINT,
    inputs TEXT,
    started_at_epoch_ms BIGINT,
    deduplication_id TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    queue_partition_key TEXT,
    forked_from TEXT,
    owner_xid TEXT DEFAULT NULL,
    parent_workflow_id TEXT DEFAULT NULL,
    serialization TEXT DEFAULT NULL,
    delay_until_epoch_ms BIGINT DEFAULT NULL,
    was_forked_from BOOLEAN NOT NULL DEFAULT FALSE,
    rate_limited BOOLEAN NOT NULL DEFAULT FALSE,
    completed_at BIGINT,
    attributes TEXT
);

CREATE INDEX workflow_status_created_at_index ON workflow_status (created_at);

CREATE INDEX idx_workflow_status_delayed ON workflow_status (delay_until_epoch_ms)
    WHERE status = 'DELAYED';

CREATE INDEX idx_workflow_status_forked_from ON workflow_status (forked_from)
    WHERE forked_from IS NOT NULL;

CREATE INDEX idx_workflow_status_parent_workflow_id ON workflow_status (parent_workflow_id)
    WHERE parent_workflow_id IS NOT NULL;

CREATE UNIQUE INDEX uq_workflow_status_dedup_id ON workflow_status (queue_name, deduplication_id)
    WHERE deduplication_id IS NOT NULL;

CREATE INDEX idx_workflow_status_pending ON workflow_status (created_at)
    WHERE status = 'PENDING';

CREATE INDEX idx_workflow_status_failed ON workflow_status (status, created_at)
    WHERE status IN ('ERROR', 'CANCELLED', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED');

CREATE INDEX idx_workflow_status_in_flight ON workflow_status (queue_name, status, priority, created_at)
    WHERE status IN ('ENQUEUED', 'PENDING');

CREATE INDEX idx_workflow_status_rate_limited ON workflow_status (queue_name, started_at_epoch_ms)
    WHERE rate_limited = TRUE;

CREATE INDEX idx_workflow_status_completed_at ON workflow_status (completed_at)
    WHERE completed_at IS NOT NULL;

CREATE INDEX idx_workflow_status_started_at ON workflow_status (started_at_epoch_ms)
    WHERE started_at_epoch_ms IS NOT NULL;

CREATE TABLE operation_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    function_name TEXT NOT NULL DEFAULT '',
    output TEXT,
    error TEXT,
    child_workflow_id TEXT,
    started_at_epoch_ms BIGINT,
    completed_at_epoch_ms BIGINT,
    serialization TEXT DEFAULT NULL,
    PRIMARY KEY (workflow_uuid, function_id),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX idx_operation_outputs_completed_at_function_name
    ON operation_outputs (completed_at_epoch_ms, function_name);

CREATE TABLE notifications (
    message_uuid TEXT NOT NULL PRIMARY KEY,
    destination_uuid TEXT NOT NULL,
    topic TEXT,
    message TEXT NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL,
    serialization TEXT DEFAULT NULL,
    consumed BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (destination_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX idx_workflow_topic ON notifications (destination_uuid, topic);

CREATE TABLE workflow_events (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    serialization TEXT DEFAULT NULL,
    PRIMARY KEY (workflow_uuid, key),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE workflow_events_history (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    serialization TEXT DEFAULT NULL,
    PRIMARY KEY (workflow_uuid, function_id, key),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE streams (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    "offset" INTEGER NOT NULL,
    function_id INTEGER NOT NULL DEFAULT 0,
    serialization TEXT DEFAULT NULL,
    PRIMARY KEY (workflow_uuid, key, "offset"),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE event_dispatch_kv (
    service_name TEXT NOT NULL,
    workflow_fn_name TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    update_seq NUMERIC,
    update_time NUMERIC,
    PRIMARY KEY (service_name, workflow_fn_name, key)
);

CREATE TABLE workflow_schedules (
    schedule_id TEXT PRIMARY KEY,
    schedule_name TEXT NOT NULL UNIQUE,
    workflow_name TEXT NOT NULL,
    workflow_class_name TEXT,
    schedule TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    context TEXT NOT NULL,
    last_fired_at TEXT DEFAULT NULL,
    automatic_backfill BOOLEAN NOT NULL DEFAULT FALSE,
    cron_timezone TEXT DEFAULT NULL,
    queue_name TEXT DEFAULT NULL
);

CREATE TABLE application_versions (
    version_id TEXT NOT NULL PRIMARY KEY,
    version_name TEXT NOT NULL UNIQUE,
    version_timestamp BIGINT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE TABLE queues (
    queue_id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    concurrency INTEGER,
    worker_concurrency INTEGER,
    rate_limit_max INTEGER,
    rate_limit_period_sec REAL,
    priority_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    partition_queue BOOLEAN NOT NULL DEFAULT FALSE,
    polling_interval_sec REAL NOT NULL DEFAULT 1.0,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
