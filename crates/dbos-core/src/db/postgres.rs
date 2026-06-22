//! Postgres [`SystemDatabase`] implementation.
//!
//! This is the
//! exact mirror of [`crate::db::sqlite`] behind the same trait: every method
//! reproduces the SQLite backend's semantics, error mapping, and column
//! handling. Differences are mechanical: Postgres uses `$N` placeholders
//! instead of `?`, real `BOOLEAN` columns, strict integer types (`INTEGER` ->
//! `i32`, `BIGINT` -> `i64`), and a dedicated `dbos` schema on the
//! `search_path`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::Row;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{Executor, PgPool, Postgres, Transaction};

use crate::constants::{DB_RETRY_INTERVAL, DBOS_INTERNAL_QUEUE_NAME, DBOS_MIGRATION_TABLE};
use crate::db::{
    AwaitResult, ForkInput, InsertWorkflowResult, InsertWorkflowStatusInput, ListWorkflowsInput,
    RecordOperationResultInput, RecordedResult, ScheduleRow, StepInfo, SystemDatabase,
    WorkflowStatus, WorkflowStatusType,
};
use crate::error::DbosError;
use crate::util::now_ms;

/// Single dialect-portable migration shared with the SQLite backend
/// (see `crate::db::MIGRATIONS`).
const MIGRATIONS: &[(i64, &str)] = crate::db::MIGRATIONS;

/// Postgres-backed system database.
pub struct PostgresDb {
    pool: PgPool,
}

impl PostgresDb {
    /// Open a Postgres system database from a URL, ensuring the `dbos` schema
    /// exists and is on the `search_path` for every connection.
    pub async fn connect(url: &str) -> Result<Arc<dyn SystemDatabase>, DbosError> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    conn.execute("SET search_path TO dbos, public").await?;
                    Ok(())
                })
            })
            .connect(url)
            .await
            .map_err(|e| {
                DbosError::initialization(format!("failed to open postgres database: {e}"))
            })?;
        // Ensure the schema exists before any connection tries to use it.
        sqlx::query("CREATE SCHEMA IF NOT EXISTS dbos")
            .execute(&pool)
            .await
            .map_err(|e| {
                DbosError::initialization(format!("failed to create dbos schema: {e}"))
            })?;
        let db = PostgresDb { pool };
        Ok(Arc::new(db))
    }

    /// Build a `PostgresDb` from an existing pool (test/embedding hook).
    pub fn from_pool(pool: PgPool) -> Arc<dyn SystemDatabase> {
        Arc::new(PostgresDb { pool })
    }

    async fn begin(&self) -> Result<Transaction<'_, Postgres>, DbosError> {
        self.pool
            .begin()
            .await
            .map_err(|e| sql_err("failed to begin transaction", e))
    }
}

fn sql_err(context: &str, e: sqlx::Error) -> DbosError {
    DbosError::new(crate::error::DbosErrorCode::WorkflowExecution, format!("{context}: {e}"))
        .with_source(e)
}

fn is_unique_violation(e: &sqlx::Error) -> bool {
    e.as_database_error().map(|d| d.is_unique_violation()).unwrap_or(false)
}

fn is_foreign_key_violation(e: &sqlx::Error) -> bool {
    e.as_database_error().map(|d| d.is_foreign_key_violation()).unwrap_or(false)
}

// --- statement splitting (mirrors sqlite.rs `split_statements`) ---
//
// The shared migration is plain portable DDL (no `$$` bodies, no schema
// qualifiers, no CONCURRENTLY), so stripping `--` comment lines and splitting on
// `;` is sufficient. Tables resolve via the connection `search_path` (`dbos`).
fn split_statements(body: &str) -> Vec<String> {
    let mut clean = String::new();
    for line in body.lines() {
        if line.trim_start().starts_with("--") {
            continue;
        }
        clean.push_str(line);
        clean.push('\n');
    }
    clean
        .split(';')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[async_trait]
impl SystemDatabase for PostgresDb {
    async fn run_migrations(&self) -> Result<(), DbosError> {
        // Ensure the migrations table exists.
        let exists: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = $1",
        )
        .bind(DBOS_MIGRATION_TABLE)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| sql_err("failed to probe information_schema", e))?;
        if exists.is_none() {
            sqlx::query(&format!(
                "CREATE TABLE {DBOS_MIGRATION_TABLE} (version BIGINT NOT NULL PRIMARY KEY)"
            ))
            .execute(&self.pool)
            .await
            .map_err(|e| sql_err("failed to create migrations table", e))?;
        }

        let mut current: i64 = sqlx::query_scalar(&format!(
            "SELECT version FROM {DBOS_MIGRATION_TABLE} LIMIT 1"
        ))
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| sql_err("failed to read migration version", e))?
        .unwrap_or(0);

        for (version, sql) in MIGRATIONS {
            if *version <= current {
                continue;
            }
            let mut tx = self.begin().await?;
            for stmt in split_statements(sql.trim()) {
                sqlx::query(&stmt)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| sql_err(&format!("failed to execute migration {version}"), e))?;
            }
            if current == 0 {
                sqlx::query(&format!("INSERT INTO {DBOS_MIGRATION_TABLE} (version) VALUES ($1)"))
                    .bind(version)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| sql_err("failed to insert migration version", e))?;
            } else {
                sqlx::query(&format!("UPDATE {DBOS_MIGRATION_TABLE} SET version = $1"))
                    .bind(version)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| sql_err("failed to update migration version", e))?;
            }
            tx.commit()
                .await
                .map_err(|e| sql_err(&format!("failed to commit migration {version}"), e))?;
            current = *version;
        }
        Ok(())
    }

    async fn insert_workflow_status(
        &self,
        input: InsertWorkflowStatusInput,
    ) -> Result<InsertWorkflowResult, DbosError> {
        let s = &input.status;
        let status = s.status.unwrap_or(WorkflowStatusType::Pending);
        // Enqueued/Delayed workflows start at 0 attempts (they have not run yet).
        let attempts: i64 =
            if matches!(status, WorkflowStatusType::Enqueued | WorkflowStatusType::Delayed) {
                0
            } else {
                1
            };
        let updated_at = s.updated_at_ms.unwrap_or_else(now_ms);
        let recovery_increment: i64 = if input.increment_attempts { 1 } else { 0 };
        let roles_json = serde_json::to_string(&s.authenticated_roles).unwrap_or_else(|_| "[]".into());

        let opt = |v: &str| if v.is_empty() { None } else { Some(v.to_string()) };

        const SQL: &str = r#"INSERT INTO workflow_status (
            workflow_uuid, status, name, queue_name, authenticated_user, assumed_role,
            authenticated_roles, executor_id, application_version, application_id,
            created_at, recovery_attempts, updated_at, workflow_timeout_ms,
            workflow_deadline_epoch_ms, inputs, deduplication_id, priority,
            queue_partition_key, owner_xid, parent_workflow_id, class_name, config_name,
            serialization, delay_until_epoch_ms
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
        ON CONFLICT (workflow_uuid) DO UPDATE SET
            recovery_attempts = CASE
                WHEN EXCLUDED.status NOT IN ($26, $27) THEN workflow_status.recovery_attempts + $28
                ELSE workflow_status.recovery_attempts
            END,
            updated_at = EXCLUDED.updated_at,
            executor_id = CASE
                WHEN EXCLUDED.status IN ($29, $30) THEN workflow_status.executor_id
                ELSE EXCLUDED.executor_id
            END
        RETURNING recovery_attempts, status, name, queue_name, queue_partition_key,
                  workflow_timeout_ms, workflow_deadline_epoch_ms, owner_xid"#;

        let mut tx = self.begin().await?;
        let row = sqlx::query(SQL)
            .bind(&s.id)
            .bind(status.as_str())
            .bind(&s.name)
            .bind(opt(&s.queue_name))
            .bind(&s.authenticated_user)
            .bind(&s.assumed_role)
            .bind(roles_json)
            .bind(&s.executor_id)
            .bind(opt(&s.application_version))
            .bind(&s.application_id)
            .bind(s.created_at_ms)
            .bind(attempts)
            .bind(updated_at)
            .bind(s.timeout_ms)
            .bind(s.deadline_ms)
            .bind(&s.input)
            .bind(opt(&s.deduplication_id))
            .bind(s.priority as i32)
            .bind(opt(&s.queue_partition_key))
            .bind(&input.owner_xid)
            .bind(opt(&s.parent_workflow_id))
            .bind(opt(&s.class_name))
            .bind(s.config_name.clone())
            .bind(&s.serialization)
            .bind(s.delay_until_ms)
            .bind(WorkflowStatusType::Enqueued.as_str())
            .bind(WorkflowStatusType::Delayed.as_str())
            .bind(recovery_increment)
            .bind(WorkflowStatusType::Enqueued.as_str())
            .bind(WorkflowStatusType::Delayed.as_str())
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| {
                if is_unique_violation(&e) {
                    DbosError::queue_deduplicated(&s.id, &s.queue_name, &s.deduplication_id)
                } else {
                    sql_err("failed to insert workflow status", e)
                }
            })?;

        let result_attempts: i64 = row.get(0);
        let result_status =
            WorkflowStatusType::parse(row.get::<String, _>(1).as_str()).unwrap_or(status);
        let result_name: Option<String> = row.get(2);
        let result_queue: Option<String> = row.get(3);
        let result_partition: Option<String> = row.get(4);
        let result_timeout: Option<i64> = row.get(5);
        let result_deadline: Option<i64> = row.get(6);
        let result_owner: Option<String> = row.get(7);

        let result = InsertWorkflowResult {
            attempts: result_attempts,
            status: result_status,
            name: result_name.clone().unwrap_or_default(),
            queue_name: result_queue.clone(),
            queue_partition_key: result_partition,
            timeout_ms: result_timeout,
            deadline_ms: result_deadline,
            owner_xid: result_owner.unwrap_or_default(),
        };

        // Name / queue conflict detection.
        if !s.name.is_empty() && result.name != s.name {
            return Err(DbosError::conflicting_workflow(
                &s.id,
                format!(
                    "Workflow already exists with a different name: {}, but the provided name is: {}",
                    result.name, s.name
                ),
            ));
        }
        if !s.queue_name.is_empty() {
            if let Some(q) = &result.queue_name {
                if q != &s.queue_name {
                    return Err(DbosError::conflicting_workflow(
                        &s.id,
                        format!(
                            "Workflow already exists in a different queue: {}, but the provided queue is: {}",
                            q, s.queue_name
                        ),
                    ));
                }
            }
        }

        // Record the parent→child mapping in the same transaction (idempotent
        // child spawning).
        if let Some(child) = &input.record_child {
            let res = sqlx::query(
                "INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, child_workflow_id)
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(&child.parent_workflow_id)
            .bind(child.step_id as i32)
            .bind(&child.step_name)
            .bind(&child.child_workflow_id)
            .execute(&mut *tx)
            .await;
            if let Err(e) = res {
                if is_unique_violation(&e) {
                    return Err(DbosError::new(
                        crate::error::DbosErrorCode::ConflictingId,
                        format!(
                            "child workflow {} already registered for parent workflow {} (operation ID: {}). Is your workflow deterministic?",
                            child.child_workflow_id, child.parent_workflow_id, child.step_id
                        ),
                    ));
                }
                return Err(sql_err("failed to record child workflow", e));
            }
        }

        // Dead-letter parking: once attempts exceed maxRetries+1, mark the
        // workflow MAX_RECOVERY_ATTEMPTS_EXCEEDED and clear queue fields.
        if result.status != WorkflowStatusType::Success
            && result.status != WorkflowStatusType::Error
            && input.max_retries > 0
            && result.attempts > input.max_retries + 1
        {
            sqlx::query(
                "UPDATE workflow_status
                 SET status = $1, deduplication_id = NULL, started_at_epoch_ms = NULL, queue_name = NULL
                 WHERE workflow_uuid = $2 AND status = $3",
            )
            .bind(WorkflowStatusType::MaxRecoveryAttemptsExceeded.as_str())
            .bind(&s.id)
            .bind(WorkflowStatusType::Pending.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| sql_err("failed to mark workflow as dead-letter", e))?;
            tx.commit()
                .await
                .map_err(|e| sql_err("failed to commit dead-letter transaction", e))?;
            return Err(DbosError::dead_letter_queue(&s.id, input.max_retries));
        }

        tx.commit()
            .await
            .map_err(|e| sql_err("failed to commit insert transaction", e))?;
        Ok(result)
    }

    async fn update_workflow_outcome(
        &self,
        workflow_id: &str,
        status: WorkflowStatusType,
        output: &Option<String>,
        err_str: &str,
    ) -> Result<(), DbosError> {
        // Forbid CANCELLED -> SUCCESS/ERROR.
        sqlx::query(
            "UPDATE workflow_status
             SET status = $1, output = $2, error = $3, updated_at = $4, completed_at = $5, deduplication_id = NULL
             WHERE workflow_uuid = $6 AND NOT (status = $7 AND $8 IN ($9, $10))",
        )
        .bind(status.as_str())
        .bind(output)
        .bind(if err_str.is_empty() { None } else { Some(err_str) })
        .bind(now_ms())
        .bind(now_ms())
        .bind(workflow_id)
        .bind(WorkflowStatusType::Cancelled.as_str())
        .bind(status.as_str())
        .bind(WorkflowStatusType::Success.as_str())
        .bind(WorkflowStatusType::Error.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| sql_err("failed to update workflow status", e))?;
        Ok(())
    }

    async fn record_operation_result(
        &self,
        input: RecordOperationResultInput,
    ) -> Result<(), DbosError> {
        let res = if input.child_workflow_id.is_some() {
            sqlx::query(
                "INSERT INTO operation_outputs
                 (workflow_uuid, function_id, output, error, function_name, started_at_epoch_ms, completed_at_epoch_ms, serialization, child_workflow_id)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            )
            .bind(&input.workflow_id)
            .bind(input.step_id as i32)
            .bind(&input.output)
            .bind(&input.error)
            .bind(&input.step_name)
            .bind(input.started_at_ms)
            .bind(input.completed_at_ms)
            .bind(&input.serialization)
            .bind(&input.child_workflow_id)
            .execute(&self.pool)
            .await
        } else {
            sqlx::query(
                "INSERT INTO operation_outputs
                 (workflow_uuid, function_id, output, error, function_name, started_at_epoch_ms, completed_at_epoch_ms, serialization)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            )
            .bind(&input.workflow_id)
            .bind(input.step_id as i32)
            .bind(&input.output)
            .bind(&input.error)
            .bind(&input.step_name)
            .bind(input.started_at_ms)
            .bind(input.completed_at_ms)
            .bind(&input.serialization)
            .execute(&self.pool)
            .await
        };
        if let Err(e) = res {
            if is_unique_violation(&e) {
                return Err(DbosError::workflow_conflict_id(&input.workflow_id));
            }
            return Err(sql_err("failed to record operation result", e));
        }
        Ok(())
    }

    async fn check_child_workflow(
        &self,
        parent_workflow_id: &str,
        step_id: i64,
    ) -> Result<Option<String>, DbosError> {
        let child: Option<Option<String>> = sqlx::query_scalar(
            "SELECT child_workflow_id FROM operation_outputs WHERE workflow_uuid = $1 AND function_id = $2",
        )
        .bind(parent_workflow_id)
        .bind(step_id as i32)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| sql_err("failed to check child workflow", e))?;
        Ok(child.flatten())
    }

    async fn check_operation_execution(
        &self,
        workflow_id: &str,
        step_id: i64,
        step_name: &str,
    ) -> Result<Option<RecordedResult>, DbosError> {
        let mut tx = self.begin().await?;
        let wf_status: Option<String> =
            sqlx::query_scalar("SELECT status FROM workflow_status WHERE workflow_uuid = $1")
                .bind(workflow_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| sql_err("failed to get workflow status", e))?;
        let wf_status = match wf_status {
            None => return Err(DbosError::non_existent_workflow(workflow_id)),
            Some(s) => s,
        };
        if wf_status == WorkflowStatusType::Cancelled.as_str() {
            return Err(DbosError::workflow_cancelled(workflow_id));
        }

        let row: Option<PgRow> = sqlx::query(
            "SELECT output, error, function_name, serialization
             FROM operation_outputs WHERE workflow_uuid = $1 AND function_id = $2",
        )
        .bind(workflow_id)
        .bind(step_id as i32)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| sql_err("failed to get operation outputs", e))?;

        let Some(row) = row else { return Ok(None) };
        let output: Option<String> = row.get(0);
        let error: Option<String> = row.get(1);
        let recorded_name: String = row.get(2);
        let serialization: Option<String> = row.get(3);

        if step_name != recorded_name {
            return Err(DbosError::unexpected_step(
                workflow_id,
                step_id,
                step_name,
                recorded_name,
            ));
        }
        let error = match error {
            Some(s) if !s.is_empty() => Some(s),
            _ => None,
        };
        Ok(Some(RecordedResult {
            output,
            error,
            serialization: serialization.unwrap_or_default(),
        }))
    }

    async fn list_workflows(
        &self,
        input: ListWorkflowsInput,
    ) -> Result<Vec<WorkflowStatus>, DbosError> {
        const BASE: &str = "SELECT workflow_uuid, status, name, authenticated_user, assumed_role, \
            authenticated_roles, executor_id, created_at, updated_at, application_version, \
            application_id, recovery_attempts, queue_name, workflow_timeout_ms, \
            workflow_deadline_epoch_ms, started_at_epoch_ms, deduplication_id, priority, \
            queue_partition_key, forked_from, parent_workflow_id, serialization, \
            delay_until_epoch_ms, was_forked_from, completed_at, class_name, config_name, \
            output, error, inputs FROM workflow_status";

        let mut clauses: Vec<String> = Vec::new();
        let mut str_binds: Vec<String> = Vec::new();
        // Postgres uses positional `$N` placeholders; track the next index.
        let mut next_param: usize = 1;

        let mut add_any = |col: &str, vals: &[String], binds: &mut Vec<String>| {
            if vals.is_empty() {
                return;
            }
            let placeholders = vals
                .iter()
                .map(|_| {
                    let p = format!("${next_param}");
                    next_param += 1;
                    p
                })
                .collect::<Vec<_>>()
                .join(", ");
            clauses.push(format!("{col} IN ({placeholders})"));
            binds.extend(vals.iter().cloned());
        };

        add_any("workflow_uuid", &input.workflow_ids, &mut str_binds);
        add_any("name", &input.workflow_name, &mut str_binds);
        let status_strs: Vec<String> =
            input.status.iter().map(|s| s.as_str().to_string()).collect();
        add_any("status", &status_strs, &mut str_binds);
        add_any("executor_id", &input.executor_ids, &mut str_binds);
        add_any("application_version", &input.application_version, &mut str_binds);
        if input.queues_only {
            clauses.push("queue_name IS NOT NULL".to_string());
        }

        let mut query = BASE.to_string();
        if !clauses.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(&clauses.join(" AND "));
        }
        query.push_str(if input.sort_desc {
            " ORDER BY created_at DESC"
        } else {
            " ORDER BY created_at ASC"
        });
        if let Some(limit) = input.limit {
            query.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = input.offset {
            query.push_str(&format!(" OFFSET {offset}"));
        }

        let mut q = sqlx::query(&query);
        for b in &str_binds {
            q = q.bind(b);
        }
        let rows = q
            .fetch_all(&self.pool)
            .await
            .map_err(|e| sql_err("failed to execute ListWorkflows query", e))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(row_to_status(&row, input.load_input, input.load_output)?);
        }
        Ok(out)
    }

    async fn get_workflow_status(
        &self,
        workflow_id: &str,
    ) -> Result<Option<WorkflowStatus>, DbosError> {
        let mut list = self
            .list_workflows(ListWorkflowsInput {
                workflow_ids: vec![workflow_id.to_string()],
                load_input: true,
                load_output: true,
                ..Default::default()
            })
            .await?;
        Ok(list.drain(..).next())
    }

    async fn await_workflow_result(
        &self,
        workflow_id: &str,
        poll_interval: Duration,
    ) -> Result<AwaitResult, DbosError> {
        let poll = if poll_interval.is_zero() { DB_RETRY_INTERVAL } else { poll_interval };
        loop {
            let row: Option<PgRow> = sqlx::query(
                "SELECT status, output, error, recovery_attempts, serialization
                 FROM workflow_status WHERE workflow_uuid = $1",
            )
            .bind(workflow_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| sql_err("failed to query workflow status", e))?;

            let Some(row) = row else {
                tokio::time::sleep(poll).await;
                continue;
            };
            let status_s: String = row.get(0);
            let output: Option<String> = row.get(1);
            let error: Option<String> = row.get(2);
            let attempts: i64 = row.get(3);
            let serialization: Option<String> = row.get(4);
            let serialization = serialization.unwrap_or_default();
            let status = WorkflowStatusType::parse(&status_s);

            match status {
                Some(WorkflowStatusType::Success) | Some(WorkflowStatusType::Error) => {
                    let error = error.filter(|s| !s.is_empty());
                    return Ok(AwaitResult { output, error, serialization });
                }
                Some(WorkflowStatusType::Cancelled) => {
                    return Err(DbosError::awaited_workflow_cancelled(workflow_id));
                }
                Some(WorkflowStatusType::MaxRecoveryAttemptsExceeded) => {
                    return Err(DbosError::dead_letter_queue(workflow_id, attempts - 2));
                }
                _ => {
                    tokio::time::sleep(poll).await;
                }
            }
        }
    }

    async fn clear_queue_assignment(&self, workflow_id: &str) -> Result<bool, DbosError> {
        let res = sqlx::query(
            "UPDATE workflow_status SET status = $1, started_at_epoch_ms = NULL
             WHERE workflow_uuid = $2 AND queue_name IS NOT NULL AND status = $3",
        )
        .bind(WorkflowStatusType::Enqueued.as_str())
        .bind(workflow_id)
        .bind(WorkflowStatusType::Pending.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| sql_err("failed to clear queue assignment", e))?;
        Ok(res.rows_affected() > 0)
    }

    async fn dequeue_workflows(
        &self,
        input: crate::db::DequeueInput,
    ) -> Result<Vec<crate::db::DequeuedWorkflow>, DbosError> {
        let mut tx = self.begin().await?;

        // Rate limiter: count workflows started within the limiter window.
        let mut recent: i64 = 0;
        if let Some((limit, period_secs)) = input.rate_limit {
            let cutoff = now_ms() - (period_secs * 1000.0) as i64;
            recent = sqlx::query_scalar(
                "SELECT COUNT(*) FROM workflow_status
                 WHERE queue_name = $1 AND rate_limited = true AND status NOT IN ($2, $3) AND started_at_epoch_ms > $4",
            )
            .bind(&input.queue_name)
            .bind(WorkflowStatusType::Enqueued.as_str())
            .bind(WorkflowStatusType::Delayed.as_str())
            .bind(cutoff)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| sql_err("failed to query rate limiter", e))?;
            if recent >= limit {
                return Ok(vec![]);
            }
        }

        // Concurrency limits.
        let mut max_tasks = input.max_tasks;
        if let Some(wc) = input.worker_concurrency {
            max_tasks = (wc - input.local_running_count).max(0);
        }
        if let Some(gc) = input.global_concurrency {
            let pending: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM workflow_status WHERE queue_name = $1 AND status = $2",
            )
            .bind(&input.queue_name)
            .bind(WorkflowStatusType::Pending.as_str())
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| sql_err("failed to query pending workflows", e))?;
            let available = (gc - pending).max(0);
            max_tasks = max_tasks.min(available);
        }
        if max_tasks <= 0 {
            return Ok(vec![]);
        }

        // Candidate ids, FIFO by priority.
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT workflow_uuid FROM workflow_status
             WHERE queue_name = $1 AND status = $2 AND (application_version = $3 OR application_version IS NULL)
             ORDER BY priority ASC, created_at ASC LIMIT $4",
        )
        .bind(&input.queue_name)
        .bind(WorkflowStatusType::Enqueued.as_str())
        .bind(&input.application_version)
        .bind(max_tasks)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| sql_err("failed to query enqueued workflows", e))?;

        let rate_limited = input.rate_limit.is_some();
        let mut out = Vec::new();
        for id in ids {
            if let Some((limit, _)) = input.rate_limit {
                if out.len() as i64 + recent >= limit {
                    break;
                }
            }
            let now = now_ms();
            let row: Option<PgRow> = sqlx::query(
                "UPDATE workflow_status
                 SET status = $1, application_version = $2, executor_id = $3, started_at_epoch_ms = $4, rate_limited = $5,
                     workflow_deadline_epoch_ms = CASE
                         WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                         THEN $6 + workflow_timeout_ms ELSE workflow_deadline_epoch_ms END
                 WHERE workflow_uuid = $7 AND status = $8
                 RETURNING name, inputs, serialization, config_name",
            )
            .bind(WorkflowStatusType::Pending.as_str())
            .bind(&input.application_version)
            .bind(&input.executor_id)
            .bind(now)
            .bind(rate_limited)
            .bind(now)
            .bind(&id)
            .bind(WorkflowStatusType::Enqueued.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| sql_err("failed to claim workflow during dequeue", e))?;
            let Some(row) = row else { continue };
            let serialization: Option<String> = row.get(2);
            out.push(crate::db::DequeuedWorkflow {
                id,
                name: row.get::<Option<String>, _>(0).unwrap_or_default(),
                input: row.get(1),
                serialization: serialization.unwrap_or_default(),
                config_name: row.get(3),
            });
        }

        if !out.is_empty() {
            tx.commit().await.map_err(|e| sql_err("failed to commit dequeue", e))?;
        }
        Ok(out)
    }

    async fn get_deduplicated_workflow(
        &self,
        queue_name: &str,
        deduplication_id: &str,
    ) -> Result<Option<String>, DbosError> {
        sqlx::query_scalar(
            "SELECT workflow_uuid FROM workflow_status WHERE queue_name = $1 AND deduplication_id = $2",
        )
        .bind(queue_name)
        .bind(deduplication_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| sql_err("failed to get deduplicated workflow", e))
    }

    async fn transition_delayed_workflows(&self) -> Result<(), DbosError> {
        sqlx::query(
            "UPDATE workflow_status SET status = $1
             WHERE status = $2 AND delay_until_epoch_ms IS NOT NULL AND delay_until_epoch_ms <= $3",
        )
        .bind(WorkflowStatusType::Enqueued.as_str())
        .bind(WorkflowStatusType::Delayed.as_str())
        .bind(now_ms())
        .execute(&self.pool)
        .await
        .map_err(|e| sql_err("failed to transition delayed workflows", e))?;
        Ok(())
    }

    async fn cancel_workflow(&self, workflow_id: &str) -> Result<bool, DbosError> {
        let now = now_ms();
        sqlx::query(
            "UPDATE workflow_status
             SET status = $1, updated_at = $2, completed_at = $3, started_at_epoch_ms = NULL,
                 queue_name = NULL, deduplication_id = NULL
             WHERE workflow_uuid = $4 AND status NOT IN ($5, $6, $7)",
        )
        .bind(WorkflowStatusType::Cancelled.as_str())
        .bind(now)
        .bind(now)
        .bind(workflow_id)
        .bind(WorkflowStatusType::Success.as_str())
        .bind(WorkflowStatusType::Error.as_str())
        .bind(WorkflowStatusType::Cancelled.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| sql_err("failed to cancel workflow", e))?;
        let exists: Option<i32> =
            sqlx::query_scalar("SELECT 1 FROM workflow_status WHERE workflow_uuid = $1")
                .bind(workflow_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| sql_err("failed to check workflow existence", e))?;
        Ok(exists.is_some())
    }

    async fn resume_workflow(&self, workflow_id: &str) -> Result<(), DbosError> {
        sqlx::query(
            "UPDATE workflow_status
             SET status = $1, queue_name = $2, recovery_attempts = 0, workflow_deadline_epoch_ms = NULL,
                 deduplication_id = NULL, started_at_epoch_ms = NULL, updated_at = $3, completed_at = NULL
             WHERE workflow_uuid = $4 AND status NOT IN ($5, $6)",
        )
        .bind(WorkflowStatusType::Enqueued.as_str())
        .bind(DBOS_INTERNAL_QUEUE_NAME)
        .bind(now_ms())
        .bind(workflow_id)
        .bind(WorkflowStatusType::Success.as_str())
        .bind(WorkflowStatusType::Error.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| sql_err("failed to resume workflow", e))?;
        Ok(())
    }

    async fn delete_workflow(&self, workflow_id: &str) -> Result<(), DbosError> {
        sqlx::query("DELETE FROM workflow_status WHERE workflow_uuid = $1")
            .bind(workflow_id)
            .execute(&self.pool)
            .await
            .map_err(|e| sql_err("failed to delete workflow", e))?;
        Ok(())
    }

    async fn get_workflow_children(&self, workflow_id: &str) -> Result<Vec<String>, DbosError> {
        sqlx::query_scalar("SELECT workflow_uuid FROM workflow_status WHERE parent_workflow_id = $1")
            .bind(workflow_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| sql_err("failed to get workflow children", e))
    }

    async fn get_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>, DbosError> {
        let rows = sqlx::query(
            "SELECT function_id, function_name, output, error, child_workflow_id,
                    started_at_epoch_ms, completed_at_epoch_ms, serialization
             FROM operation_outputs WHERE workflow_uuid = $1 ORDER BY function_id ASC",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| sql_err("failed to query workflow steps", e))?;
        Ok(rows
            .into_iter()
            .map(|r| StepInfo {
                // `function_id` is INTEGER in Postgres; read as i32 and widen.
                step_id: r.get::<i32, _>(0) as i64,
                step_name: r.get::<Option<String>, _>(1).unwrap_or_default(),
                output: r.get(2),
                error: r.get::<Option<String>, _>(3).filter(|s| !s.is_empty()),
                child_workflow_id: r.get(4),
                started_at_ms: r.get(5),
                completed_at_ms: r.get(6),
                serialization: r.get::<Option<String>, _>(7).unwrap_or_default(),
            })
            .collect())
    }

    async fn fork_workflow(&self, input: ForkInput) -> Result<String, DbosError> {
        if input.start_step < 0 {
            return Err(DbosError::new(
                crate::error::DbosErrorCode::WorkflowExecution,
                format!("startStep must be >= 0, got {}", input.start_step),
            ));
        }
        let forked_id = input.forked_workflow_id.clone().unwrap_or_else(crate::util::new_uuid);
        let queue = input.queue_name.clone().unwrap_or_else(|| DBOS_INTERNAL_QUEUE_NAME.to_string());

        let orig = self
            .get_workflow_status(&input.original_workflow_id)
            .await?
            .ok_or_else(|| DbosError::non_existent_workflow(&input.original_workflow_id))?;
        let app_version = input.application_version.clone().unwrap_or(orig.application_version);
        let roles = serde_json::to_string(&orig.authenticated_roles).unwrap_or_else(|_| "[]".into());
        let now = now_ms();

        let mut tx = self.begin().await?;
        sqlx::query(
            "INSERT INTO workflow_status (workflow_uuid, status, name, authenticated_user, assumed_role,
                authenticated_roles, application_version, application_id, queue_name, inputs,
                created_at, updated_at, recovery_attempts, forked_from, serialization, class_name, config_name)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,0,$13,$14,$15,$16)",
        )
        .bind(&forked_id)
        .bind(WorkflowStatusType::Enqueued.as_str())
        .bind(&orig.name)
        .bind(&orig.authenticated_user)
        .bind(&orig.assumed_role)
        .bind(roles)
        .bind(&app_version)
        .bind(&orig.application_id)
        .bind(&queue)
        .bind(&orig.input)
        .bind(now)
        .bind(now)
        .bind(&input.original_workflow_id)
        .bind(&orig.serialization)
        .bind(if orig.class_name.is_empty() { None } else { Some(orig.class_name.clone()) })
        .bind(orig.config_name.clone())
        .execute(&mut *tx)
        .await
        .map_err(|e| sql_err("failed to insert forked workflow", e))?;

        sqlx::query("UPDATE workflow_status SET was_forked_from = true WHERE workflow_uuid = $1")
            .bind(&input.original_workflow_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| sql_err("failed to mark original as forked", e))?;

        if input.start_step > 0 {
            sqlx::query(
                "INSERT INTO operation_outputs (workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization)
                 SELECT $1, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization
                 FROM operation_outputs WHERE workflow_uuid = $2 AND function_id < $3",
            )
            .bind(&forked_id)
            .bind(&input.original_workflow_id)
            .bind(input.start_step as i32)
            .execute(&mut *tx)
            .await
            .map_err(|e| sql_err("failed to copy operation outputs", e))?;
        }

        tx.commit().await.map_err(|e| sql_err("failed to commit fork", e))?;
        Ok(forked_id)
    }

    async fn get_workflow_status_counts(&self) -> Result<Vec<(String, i64)>, DbosError> {
        let rows = sqlx::query("SELECT status, COUNT(*) FROM workflow_status GROUP BY status")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| sql_err("failed to aggregate workflow counts", e))?;
        Ok(rows.into_iter().map(|r| (r.get::<String, _>(0), r.get::<i64, _>(1))).collect())
    }

    async fn garbage_collect(
        &self,
        cutoff_epoch_ms: Option<i64>,
        rows_threshold: Option<i64>,
    ) -> Result<u64, DbosError> {
        let mut removed = 0u64;
        if let Some(cutoff) = cutoff_epoch_ms {
            let r = sqlx::query(
                "DELETE FROM workflow_status
                 WHERE status IN ($1, $2, $3) AND COALESCE(completed_at, updated_at) < $4",
            )
            .bind(WorkflowStatusType::Success.as_str())
            .bind(WorkflowStatusType::Error.as_str())
            .bind(WorkflowStatusType::Cancelled.as_str())
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .map_err(|e| sql_err("failed to garbage collect by cutoff", e))?;
            removed += r.rows_affected();
        }
        if let Some(threshold) = rows_threshold {
            // Postgres does not support `LIMIT -1`; use a bare OFFSET subquery to
            // keep only the newest `threshold` terminal workflows.
            let r = sqlx::query(
                "DELETE FROM workflow_status WHERE workflow_uuid IN (
                     SELECT workflow_uuid FROM workflow_status
                     WHERE status IN ($1, $2, $3)
                     ORDER BY created_at DESC OFFSET $4)",
            )
            .bind(WorkflowStatusType::Success.as_str())
            .bind(WorkflowStatusType::Error.as_str())
            .bind(WorkflowStatusType::Cancelled.as_str())
            .bind(threshold)
            .execute(&self.pool)
            .await
            .map_err(|e| sql_err("failed to garbage collect by threshold", e))?;
            removed += r.rows_affected();
        }
        Ok(removed)
    }

    async fn set_workflow_status_pending(&self, workflow_id: &str) -> Result<(), DbosError> {
        sqlx::query(
            "UPDATE workflow_status
             SET status = $1, output = NULL, error = NULL, started_at_epoch_ms = NULL, updated_at = $2
             WHERE workflow_uuid = $3",
        )
        .bind(WorkflowStatusType::Pending.as_str())
        .bind(now_ms())
        .bind(workflow_id)
        .execute(&self.pool)
        .await
        .map_err(|e| sql_err("failed to set workflow status pending", e))?;
        Ok(())
    }

    async fn send_notification(
        &self,
        dest: &str,
        topic: &str,
        message: &Option<String>,
        serialization: &str,
    ) -> Result<(), DbosError> {
        let res = sqlx::query(
            "INSERT INTO notifications (destination_uuid, topic, message, serialization, message_uuid, created_at_epoch_ms)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(dest)
        .bind(topic)
        .bind(message)
        .bind(serialization)
        .bind(crate::util::new_uuid())
        .bind(now_ms())
        .execute(&self.pool)
        .await;
        if let Err(e) = res {
            if is_foreign_key_violation(&e) {
                return Err(DbosError::non_existent_workflow(dest));
            }
            return Err(sql_err("failed to insert notification", e));
        }
        Ok(())
    }

    async fn has_unconsumed_notification(
        &self,
        dest: &str,
        topic: &str,
    ) -> Result<bool, DbosError> {
        let exists: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM notifications WHERE destination_uuid = $1 AND topic = $2 AND consumed = false LIMIT 1",
        )
        .bind(dest)
        .bind(topic)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| sql_err("failed to check notification", e))?;
        Ok(exists.is_some())
    }

    async fn consume_oldest_notification(
        &self,
        dest: &str,
        topic: &str,
    ) -> Result<Option<(Option<String>, String)>, DbosError> {
        let mut tx = self.begin().await?;
        let uuid: Option<String> = sqlx::query_scalar(
            "SELECT message_uuid FROM notifications
             WHERE destination_uuid = $1 AND topic = $2 AND consumed = false
             ORDER BY created_at_epoch_ms ASC LIMIT 1",
        )
        .bind(dest)
        .bind(topic)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| sql_err("failed to find oldest notification", e))?;
        let Some(uuid) = uuid else {
            return Ok(None);
        };
        let row: PgRow = sqlx::query(
            "UPDATE notifications SET consumed = true WHERE message_uuid = $1 RETURNING message, serialization",
        )
        .bind(&uuid)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| sql_err("failed to consume notification", e))?;
        let message: Option<String> = row.get(0);
        let serialization: Option<String> = row.get(1);
        tx.commit().await.map_err(|e| sql_err("failed to commit consume", e))?;
        Ok(Some((message, serialization.unwrap_or_default())))
    }

    async fn set_event(
        &self,
        workflow_id: &str,
        function_id: i64,
        key: &str,
        value: &Option<String>,
        serialization: &str,
    ) -> Result<(), DbosError> {
        let mut tx = self.begin().await?;
        sqlx::query(
            "INSERT INTO workflow_events (workflow_uuid, key, value, serialization)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (workflow_uuid, key) DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization",
        )
        .bind(workflow_id)
        .bind(key)
        .bind(value)
        .bind(serialization)
        .execute(&mut *tx)
        .await
        .map_err(|e| sql_err("failed to set event", e))?;
        sqlx::query(
            "INSERT INTO workflow_events_history (workflow_uuid, function_id, key, value, serialization)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (workflow_uuid, function_id, key) DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization",
        )
        .bind(workflow_id)
        .bind(function_id as i32)
        .bind(key)
        .bind(value)
        .bind(serialization)
        .execute(&mut *tx)
        .await
        .map_err(|e| sql_err("failed to record event history", e))?;
        tx.commit().await.map_err(|e| sql_err("failed to commit set_event", e))?;
        Ok(())
    }

    async fn get_event(
        &self,
        target_workflow_id: &str,
        key: &str,
    ) -> Result<Option<(Option<String>, String)>, DbosError> {
        let row: Option<PgRow> = sqlx::query(
            "SELECT value, serialization FROM workflow_events WHERE workflow_uuid = $1 AND key = $2",
        )
        .bind(target_workflow_id)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| sql_err("failed to get event", e))?;
        Ok(row.map(|r| {
            let value: Option<String> = r.get(0);
            let serialization: Option<String> = r.get(1);
            (value, serialization.unwrap_or_default())
        }))
    }

    async fn write_stream(
        &self,
        workflow_id: &str,
        key: &str,
        value: &str,
        function_id: i64,
        serialization: &str,
    ) -> Result<(), DbosError> {
        let closed: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM streams WHERE workflow_uuid = $1 AND key = $2 AND value = $3 LIMIT 1",
        )
        .bind(workflow_id)
        .bind(key)
        .bind(crate::constants::DBOS_STREAM_CLOSED_SENTINEL)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| sql_err("failed to check stream status", e))?;
        if closed.is_some() {
            return Err(DbosError::new(
                crate::error::DbosErrorCode::WorkflowExecution,
                format!("stream '{key}' is already closed"),
            ));
        }
        sqlx::query(
            "INSERT INTO streams (workflow_uuid, key, value, \"offset\", function_id, serialization)
             SELECT $1, $2, $3, COALESCE((SELECT MAX(\"offset\") FROM streams WHERE workflow_uuid = $1 AND key = $2), -1) + 1, $4, $5",
        )
        .bind(workflow_id)
        .bind(key)
        .bind(value)
        .bind(function_id as i32)
        .bind(serialization)
        .execute(&self.pool)
        .await
        .map_err(|e| sql_err("failed to insert stream entry", e))?;
        Ok(())
    }

    async fn read_stream(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i64,
    ) -> Result<(Vec<(String, i64, String)>, bool), DbosError> {
        let rows = sqlx::query(
            "SELECT value, \"offset\", serialization FROM streams
             WHERE workflow_uuid = $1 AND key = $2 AND \"offset\" >= $3 ORDER BY \"offset\" ASC",
        )
        .bind(workflow_id)
        .bind(key)
        .bind(from_offset as i32)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| sql_err("failed to query stream", e))?;
        let mut out = Vec::new();
        let mut closed = false;
        for r in rows {
            let value: String = r.get(0);
            if value == crate::constants::DBOS_STREAM_CLOSED_SENTINEL {
                closed = true;
                break;
            }
            let offset: i32 = r.get(1);
            let ser: Option<String> = r.get(2);
            out.push((value, offset as i64, ser.unwrap_or_default()));
        }
        Ok((out, closed))
    }

    async fn create_application_version(&self, version_name: &str) -> Result<(), DbosError> {
        let now = now_ms();
        sqlx::query(
            "INSERT INTO application_versions (version_id, version_name, version_timestamp, created_at)
             VALUES ($1, $2, $3, $4) ON CONFLICT (version_name) DO NOTHING",
        )
        .bind(crate::util::new_uuid())
        .bind(version_name)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|e| sql_err("failed to create application version", e))?;
        Ok(())
    }

    async fn get_latest_application_version(&self) -> Result<Option<String>, DbosError> {
        let name: Option<String> = sqlx::query_scalar(
            "SELECT version_name FROM application_versions ORDER BY version_timestamp DESC LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| sql_err("failed to get latest application version", e))?;
        Ok(name)
    }

    async fn get_workflow_events(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<(String, Option<String>, String)>, DbosError> {
        let rows = sqlx::query(
            "SELECT key, value, serialization FROM workflow_events WHERE workflow_uuid = $1",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| sql_err("failed to get workflow events", e))?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.get::<String, _>(0),
                    r.get::<Option<String>, _>(1),
                    r.get::<Option<String>, _>(2).unwrap_or_default(),
                )
            })
            .collect())
    }

    async fn get_workflow_notifications(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<(String, Option<String>, i64, String, bool)>, DbosError> {
        let rows = sqlx::query(
            "SELECT topic, message, created_at_epoch_ms, serialization, consumed
             FROM notifications WHERE destination_uuid = $1 ORDER BY created_at_epoch_ms ASC",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| sql_err("failed to get workflow notifications", e))?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.get::<Option<String>, _>(0).unwrap_or_default(),
                    r.get::<Option<String>, _>(1),
                    r.get::<i64, _>(2),
                    r.get::<Option<String>, _>(3).unwrap_or_default(),
                    r.get::<bool, _>(4),
                )
            })
            .collect())
    }

    async fn get_workflow_streams(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<(String, i64, String, String)>, DbosError> {
        let rows = sqlx::query(
            "SELECT key, \"offset\", value, serialization FROM streams
             WHERE workflow_uuid = $1 ORDER BY key ASC, \"offset\" ASC",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| sql_err("failed to get workflow streams", e))?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.get::<String, _>(0),
                    // `offset` is INTEGER in Postgres; read as i32 and widen.
                    r.get::<i32, _>(1) as i64,
                    r.get::<String, _>(2),
                    r.get::<Option<String>, _>(3).unwrap_or_default(),
                )
            })
            .collect())
    }

    async fn get_step_aggregates(&self) -> Result<Vec<(String, i64)>, DbosError> {
        let rows = sqlx::query(
            "SELECT function_name, COUNT(*) FROM operation_outputs GROUP BY function_name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| sql_err("failed to aggregate step counts", e))?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<Option<String>, _>(0).unwrap_or_default(), r.get::<i64, _>(1)))
            .collect())
    }

    async fn list_application_versions(
        &self,
    ) -> Result<Vec<(String, String, i64, i64)>, DbosError> {
        let rows = sqlx::query(
            "SELECT version_id, version_name, version_timestamp, created_at
             FROM application_versions ORDER BY version_timestamp DESC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| sql_err("failed to list application versions", e))?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.get::<String, _>(0),
                    r.get::<String, _>(1),
                    r.get::<i64, _>(2),
                    r.get::<i64, _>(3),
                )
            })
            .collect())
    }

    async fn set_latest_application_version(
        &self,
        version_name: &str,
    ) -> Result<(), DbosError> {
        sqlx::query("UPDATE application_versions SET version_timestamp = $1 WHERE version_name = $2")
            .bind(now_ms())
            .bind(version_name)
            .execute(&self.pool)
            .await
            .map_err(|e| sql_err("failed to set latest application version", e))?;
        Ok(())
    }

    async fn create_schedule(&self, row: ScheduleRow) -> Result<(), DbosError> {
        let res = sqlx::query(
            "INSERT INTO workflow_schedules (
                schedule_id, schedule_name, workflow_name, workflow_class_name,
                schedule, context, status, automatic_backfill, cron_timezone, queue_name
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        )
        .bind(&row.schedule_id)
        .bind(&row.schedule_name)
        .bind(&row.workflow_name)
        .bind(&row.workflow_class_name)
        .bind(&row.schedule)
        .bind(&row.context)
        .bind(&row.status)
        .bind(row.automatic_backfill)
        .bind(&row.cron_timezone)
        .bind(&row.queue_name)
        .execute(&self.pool)
        .await;
        if let Err(e) = res {
            if is_unique_violation(&e) {
                return Err(DbosError::new(
                    crate::error::DbosErrorCode::ConflictingId,
                    format!("schedule already exists: {}", row.schedule_name),
                ));
            }
            return Err(sql_err("failed to create schedule", e));
        }
        Ok(())
    }

    async fn list_schedules(&self) -> Result<Vec<ScheduleRow>, DbosError> {
        let rows = sqlx::query(
            "SELECT schedule_id, schedule_name, workflow_name, workflow_class_name,
                    schedule, status, context, last_fired_at, automatic_backfill,
                    cron_timezone, queue_name
             FROM workflow_schedules",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| sql_err("failed to list schedules", e))?;
        Ok(rows.iter().map(row_to_schedule).collect())
    }

    async fn get_schedule(&self, name: &str) -> Result<Option<ScheduleRow>, DbosError> {
        let row: Option<PgRow> = sqlx::query(
            "SELECT schedule_id, schedule_name, workflow_name, workflow_class_name,
                    schedule, status, context, last_fired_at, automatic_backfill,
                    cron_timezone, queue_name
             FROM workflow_schedules WHERE schedule_name = $1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| sql_err("failed to get schedule", e))?;
        Ok(row.as_ref().map(row_to_schedule))
    }

    async fn set_schedule_status(&self, name: &str, status: &str) -> Result<(), DbosError> {
        // This single UPDATE resets last_fired_at to NULL.
        sqlx::query(
            "UPDATE workflow_schedules SET status = $1, last_fired_at = NULL WHERE schedule_name = $2",
        )
        .bind(status)
        .bind(name)
        .execute(&self.pool)
        .await
        .map_err(|e| sql_err("failed to set schedule status", e))?;
        Ok(())
    }

    async fn update_schedule_last_fired(
        &self,
        name: &str,
        last_fired_at: &str,
    ) -> Result<(), DbosError> {
        sqlx::query("UPDATE workflow_schedules SET last_fired_at = $1 WHERE schedule_name = $2")
            .bind(last_fired_at)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| sql_err("failed to update schedule last_fired_at", e))?;
        Ok(())
    }

    async fn delete_schedule(&self, name: &str) -> Result<(), DbosError> {
        sqlx::query("DELETE FROM workflow_schedules WHERE schedule_name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| sql_err("failed to delete schedule", e))?;
        Ok(())
    }

    async fn close(&self) {
        self.pool.close().await;
    }
}

/// Map a `workflow_schedules` row (fixed column order) to [`ScheduleRow`].
fn row_to_schedule(row: &PgRow) -> ScheduleRow {
    ScheduleRow {
        schedule_id: row.get(0),
        schedule_name: row.get(1),
        workflow_name: row.get(2),
        workflow_class_name: row.get(3),
        schedule: row.get(4),
        status: row.get(5),
        context: row.get(6),
        last_fired_at: row.get(7),
        automatic_backfill: row.try_get::<bool, _>(8).unwrap_or(false),
        cron_timezone: row.get(9),
        queue_name: row.get(10),
    }
}

/// Map a `workflow_status` row (fixed column order) to [`WorkflowStatus`].
fn row_to_status(
    row: &PgRow,
    load_input: bool,
    load_output: bool,
) -> Result<WorkflowStatus, DbosError> {
    let get_str = |idx: usize| -> String { row.get::<Option<String>, _>(idx).unwrap_or_default() };
    let roles_raw: Option<String> = row.get(5);
    let authenticated_roles: Vec<String> = roles_raw
        .filter(|s| !s.is_empty())
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    let status = WorkflowStatusType::parse(&row.get::<String, _>(1))
        .ok_or_else(|| DbosError::initialization("invalid workflow status in row"))?;

    Ok(WorkflowStatus {
        id: row.get(0),
        status,
        name: get_str(2),
        authenticated_user: get_str(3),
        assumed_role: get_str(4),
        authenticated_roles,
        executor_id: get_str(6),
        created_at_ms: row.get(7),
        updated_at_ms: row.get(8),
        application_version: get_str(9),
        application_id: get_str(10),
        attempts: row.get(11),
        queue_name: get_str(12),
        timeout_ms: row.get::<Option<i64>, _>(13).unwrap_or(0),
        deadline_ms: row.get(14),
        started_at_ms: row.get(15),
        deduplication_id: get_str(16),
        // `priority` is INTEGER in Postgres; read as i32 and widen.
        priority: row.get::<Option<i32>, _>(17).unwrap_or(0) as i64,
        queue_partition_key: get_str(18),
        forked_from: get_str(19),
        parent_workflow_id: get_str(20),
        serialization: get_str(21),
        delay_until_ms: row.get(22),
        was_forked_from: row.try_get::<bool, _>(23).unwrap_or(false),
        completed_at_ms: row.get(24),
        class_name: get_str(25),
        config_name: row.get(26),
        output: if load_output { row.get(27) } else { None },
        error: if load_output {
            row.get::<Option<String>, _>(28).filter(|s| !s.is_empty())
        } else {
            None
        },
        input: if load_input { row.get(29) } else { None },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::NewWorkflowStatus;

    const TEST_URL: &str = "postgres://dbos:dbos@localhost:5433/dbos";

    /// Connect, drop the `dbos` schema for a clean slate, and run migrations.
    async fn fresh() -> Arc<dyn SystemDatabase> {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    conn.execute("SET search_path TO dbos, public").await?;
                    Ok(())
                })
            })
            .connect(TEST_URL)
            .await
            .expect("connect to test postgres");
        sqlx::query("DROP SCHEMA IF EXISTS dbos CASCADE")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE SCHEMA dbos").execute(&pool).await.unwrap();
        let db = PostgresDb::from_pool(pool);
        db.run_migrations().await.unwrap();
        db
    }

    // Requires a live Postgres at TEST_URL; run explicitly with
    // `cargo test -p dbos-core --lib db::postgres -- --ignored`.
    #[tokio::test]
    #[ignore = "requires a live Postgres server"]
    async fn migrate_insert_and_readback() {
        let db = fresh().await;
        let r = db
            .insert_workflow_status(InsertWorkflowStatusInput {
                status: NewWorkflowStatus {
                    id: "pg-wf-1".into(),
                    status: Some(WorkflowStatusType::Pending),
                    name: "my_wf".into(),
                    executor_id: "local".into(),
                    application_version: "v1".into(),
                    created_at_ms: now_ms(),
                    input: Some("aW5wdXQ=".into()),
                    serialization: "DBOS_JSON".into(),
                    ..Default::default()
                },
                max_retries: 100,
                owner_xid: "owner-1".into(),
                increment_attempts: false,
                record_child: None,
            })
            .await
            .unwrap();
        assert_eq!(r.attempts, 1);
        assert_eq!(r.status, WorkflowStatusType::Pending);
        assert_eq!(r.owner_xid, "owner-1");

        let st = db.get_workflow_status("pg-wf-1").await.unwrap().unwrap();
        assert_eq!(st.id, "pg-wf-1");
        assert_eq!(st.name, "my_wf");
        assert_eq!(st.status, WorkflowStatusType::Pending);
        assert_eq!(st.application_version, "v1");

        db.close().await;
    }
}
