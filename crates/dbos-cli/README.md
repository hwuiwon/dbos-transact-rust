# dbos-cli

The `dbos` command-line tool for [`dbos-core`](https://crates.io/crates/dbos-core): serve the admin
API and inspect/manage workflows.

> A Rust port of [**DBOS Transact**](https://github.com/dbos-inc) by [DBOS, Inc.](https://www.dbos.dev)
> Independent, community implementation.

```sh
cargo install dbos-cli

dbos serve --port 3001 --database-url postgres://localhost/myapp
dbos workflow list --status PENDING
dbos workflow get <id>
dbos workflow cancel <id>
dbos workflow resume <id>
```

Global options: `--database-url` (env `DBOS_DATABASE_URL`) and `--app-name`.

See the [project README](https://github.com/hwuiwon/dbos-transact-rust) for details.

## License

MIT.
