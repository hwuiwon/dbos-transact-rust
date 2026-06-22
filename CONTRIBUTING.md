# Contributing

Thanks for your interest in contributing! Contributions of all kinds are welcome — bug reports,
docs, tests, and code.

## Getting started

```sh
# fork the repo on GitHub, then:
git clone https://github.com/<your-username>/dbos-transact-rust
cd dbos-transact-rust
cargo build --workspace
cargo test --workspace        # runs against in-process SQLite, no services needed
```

You need a recent stable Rust toolchain (see `rust-version` in `Cargo.toml`).

## Development workflow

This repository uses a **fork + pull request** model, and `main` is protected:

1. Fork the repo and create a topic branch (`git checkout -b my-change`).
2. Make your change with tests.
3. Make sure the checks below pass locally.
4. Push to your fork and open a pull request against `main`.
5. A maintainer review is **required** before a PR can be merged, and all CI checks must pass.

Please keep PRs focused; open an issue first for large or design-affecting changes.

## Checks (must pass)

```sh
cargo fmt --all --check                       # formatting
cargo clippy --workspace --all-targets -- -D warnings   # lints
cargo test --workspace                        # tests (SQLite)
```

To also run the suite against Postgres (optional locally; CI does it):

```sh
docker run -d --name dbos-pg -e POSTGRES_PASSWORD=dbos -e POSTGRES_USER=dbos \
  -e POSTGRES_DB=dbos -p 5433:5432 postgres:17
DBOS_TEST_DATABASE_URL='postgres://dbos:dbos@localhost:5433' \
  cargo test -p dbos-core -- --test-threads=4
```

## Guidelines

- Match the surrounding code style; `cargo fmt` is the source of truth.
- Add tests for new behavior; keep the default test suite free of external-service dependencies
  (gate anything that needs Postgres behind `DBOS_TEST_DATABASE_URL`/`#[ignore]`).
- Write clear doc comments on public items.
- Keep commits reasonably scoped with descriptive messages.

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
