//! `dbos` — command-line interface for DBOS Transact.
//!
//! Connects to a DBOS system database via [`dbos::Client`] to inspect and manage
//! workflows, and can run the admin HTTP server (`dbos serve`). Built on the
//! `dbos-core` and `dbos-server` public APIs.

use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use dbos::{Client, ClientConfig, Config, ListWorkflowsInput, WorkflowStatus, WorkflowStatusType};

/// Top-level CLI.
#[derive(Debug, Parser)]
#[command(name = "dbos", version, about = "DBOS Transact command-line interface")]
struct Cli {
    #[command(flatten)]
    global: GlobalArgs,

    #[command(subcommand)]
    command: Command,
}

/// Options shared by every subcommand.
#[derive(Debug, Args)]
struct GlobalArgs {
    /// System database connection URL.
    #[arg(long, global = true, env = "DBOS_DATABASE_URL")]
    database_url: Option<String>,

    /// Application name.
    #[arg(long, global = true, default_value = "dbos-cli")]
    app_name: String,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect and manage workflows.
    Workflow {
        #[command(subcommand)]
        command: WorkflowCommand,
    },
    /// Run the admin HTTP server until Ctrl-C.
    Serve {
        /// Port to bind the admin server on.
        #[arg(long, default_value_t = 3001)]
        port: u16,
    },
}

#[derive(Debug, Subcommand)]
enum WorkflowCommand {
    /// List workflows.
    List {
        /// Filter by status (e.g. PENDING, SUCCESS, ERROR).
        #[arg(long)]
        status: Option<String>,
        /// Filter by workflow name.
        #[arg(long)]
        name: Option<String>,
        /// Maximum number of rows to return.
        #[arg(long)]
        limit: Option<i64>,
    },
    /// Show a single workflow's status.
    Get {
        /// Workflow id.
        id: String,
    },
    /// List a workflow's executed steps.
    Steps {
        /// Workflow id.
        id: String,
    },
    /// Cancel a workflow.
    Cancel {
        /// Workflow id.
        id: String,
    },
    /// Resume a cancelled/failed workflow.
    Resume {
        /// Workflow id.
        id: String,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;

    runtime.block_on(run(cli))
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Workflow { command } => run_workflow(&cli.global, command).await,
        Command::Serve { port } => run_serve(&cli.global, port).await,
    }
}

/// Connect a [`Client`] using the global args.
async fn connect(global: &GlobalArgs) -> Result<Client> {
    let database_url = global
        .database_url
        .clone()
        .context("a database URL is required (--database-url or DBOS_DATABASE_URL)")?;
    Client::new(ClientConfig {
        app_name: global.app_name.clone(),
        database_url,
        ..Default::default()
    })
    .await
    .context("failed to connect to the DBOS system database")
}

async fn run_workflow(global: &GlobalArgs, command: WorkflowCommand) -> Result<()> {
    let client = connect(global).await?;
    match command {
        WorkflowCommand::List {
            status,
            name,
            limit,
        } => {
            let input = ListWorkflowsInput {
                status: status
                    .as_deref()
                    .and_then(WorkflowStatusType::parse)
                    .map(|s| vec![s])
                    .unwrap_or_default(),
                workflow_name: name.map(|n| vec![n]).unwrap_or_default(),
                limit,
                sort_desc: true,
                ..Default::default()
            };
            let workflows = client
                .list_workflows(input)
                .await
                .context("failed to list workflows")?;
            print_workflow_table(&workflows);
        }
        WorkflowCommand::Get { id } => {
            let input = ListWorkflowsInput {
                workflow_ids: vec![id.clone()],
                load_input: true,
                load_output: true,
                ..Default::default()
            };
            let workflows = client
                .list_workflows(input)
                .await
                .context("failed to get workflow")?;
            match workflows.first() {
                Some(ws) => print_workflow_detail(ws),
                None => anyhow::bail!("workflow {id} not found"),
            }
        }
        WorkflowCommand::Steps { id } => {
            let steps = client
                .get_workflow_steps(&id)
                .await
                .context("failed to get workflow steps")?;
            println!("{:<10} {:<30} {:<20}", "STEP", "NAME", "CHILD_WORKFLOW");
            for step in &steps {
                println!(
                    "{:<10} {:<30} {:<20}",
                    step.step_id,
                    step.step_name,
                    step.child_workflow_id.clone().unwrap_or_default()
                );
            }
        }
        WorkflowCommand::Cancel { id } => {
            client
                .cancel_workflow(&id)
                .await
                .context("failed to cancel workflow")?;
            println!("cancelled {id}");
        }
        WorkflowCommand::Resume { id } => {
            let handle = client
                .resume_workflow::<serde_json::Value>(&id)
                .await
                .context("failed to resume workflow")?;
            println!("resumed {}", handle.workflow_id());
        }
    }
    client.shutdown(Duration::from_secs(5)).await;
    Ok(())
}

async fn run_serve(global: &GlobalArgs, port: u16) -> Result<()> {
    let database_url = global
        .database_url
        .clone()
        .context("a database URL is required (--database-url or DBOS_DATABASE_URL)")?;
    let ctx = dbos::new_context(Config {
        app_name: global.app_name.clone(),
        database_url: Some(database_url),
        ..Default::default()
    })
    .await
    .context("failed to build DBOS context")?;
    ctx.launch().await.context("failed to launch DBOS")?;

    let handle = dbos_server::start_admin_server(ctx.clone(), port)
        .await
        .context("failed to start admin server")?;
    tracing::info!(addr = %handle.local_addr(), "admin server running; press Ctrl-C to stop");

    tokio::signal::ctrl_c()
        .await
        .context("failed to listen for Ctrl-C")?;
    tracing::info!("shutting down");
    handle.shutdown().await;
    ctx.shutdown(Duration::from_secs(10)).await;
    Ok(())
}

/// Print a compact table of workflows.
fn print_workflow_table(workflows: &[WorkflowStatus]) {
    println!(
        "{:<38} {:<30} {:<12} {:<20}",
        "ID", "NAME", "STATUS", "QUEUE"
    );
    for ws in workflows {
        println!(
            "{:<38} {:<30} {:<12} {:<20}",
            ws.id,
            ws.name,
            ws.status.as_str(),
            ws.queue_name
        );
    }
}

/// Print a single workflow's key fields.
fn print_workflow_detail(ws: &WorkflowStatus) {
    println!("ID:                {}", ws.id);
    println!("Name:              {}", ws.name);
    println!("Status:            {}", ws.status.as_str());
    println!("Queue:             {}", ws.queue_name);
    println!("Executor:          {}", ws.executor_id);
    println!("ApplicationVersion:{}", ws.application_version);
    println!("Attempts:          {}", ws.attempts);
    println!("CreatedAt(ms):     {}", ws.created_at_ms);
    println!("UpdatedAt(ms):     {}", ws.updated_at_ms);
    if let Some(input) = &ws.input {
        println!("Input:             {input}");
    }
    if let Some(output) = &ws.output {
        println!("Output:            {output}");
    }
    if let Some(error) = &ws.error {
        println!("Error:             {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_is_well_formed() {
        Cli::command().debug_assert();
    }
}
