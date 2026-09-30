# Contributing

This guide covers the development setup and what a pull request needs before it can be merged.

## Setup

- **Rust 1.88 or newer**, installed with [rustup](https://rustup.rs).
- **`protoc`**, to build the `grpc` feature.
  - macOS: `brew install protobuf`
  - Debian and Ubuntu: `sudo apt-get install protobuf-compiler`
- **[cargo-hack](https://github.com/taiki-e/cargo-hack) and [cargo-deny](https://github.com/EmbarkStudios/cargo-deny)**, to run the full set of CI checks locally. Install both with `make tools`.

## Checks

The Makefile runs the same commands as CI. `make help` lists every target.

| Command | Description |
|---|---|
| `make fmt` | Format the code |
| `make lint` | Run clippy on all targets with all features |
| `make test` | Run the tests with all features |
| `make doc` | Build the documentation |
| `make features` | Run clippy on every combination of features |
| `make deny` | Check dependencies for security advisories and license issues |
| `make msrv` | Check the build on the minimum supported Rust version |
| `make ci` | Run every check above except `fmt` and `msrv` |

Warnings fail every check, including missing documentation on public items. Run `make ci` before opening a pull request.

## Benchmarks

The benchmarks in `benches/` use [Criterion](https://github.com/criterion-rs/criterion.rs). `make bench` runs all of them; `cargo bench --bench engine` runs one file, and `cargo bench -- depth` runs only the benchmarks whose names contain `depth`.

| File | Measures |
|---|---|
| `broker.rs` | The in-memory broker on its own: an enqueue, claim, and ack cycle at several queue depths, and several producers feeding one consumer |
| `engine.rs` | End-to-end task throughput at several concurrency limits |
| `payload.rs` | End-to-end throughput with payloads from 100 bytes to 1 MB |

Criterion compares each run with the previous one on the same machine and writes an HTML report to `target/criterion/report/index.html`. To measure a change, record a baseline on `main` and compare your branch against it:

```sh
git switch main && cargo bench -- --save-baseline main
git switch my-branch && cargo bench -- --baseline main
```

Run benchmarks on an otherwise idle machine, preferably on AC power, and only compare results taken on the same machine. CI compiles the benchmarks but does not run them, because timings on shared CI runners vary too much to be useful.

## Pull requests

- Keep each pull request to a single change. For a large change or a change to the public API, open an issue first so the design can be agreed on before the work starts.
- Add tests for bug fixes and new behavior.
- Add an entry under `Unreleased` in [CHANGELOG.md](CHANGELOG.md) for any user-visible change.
- Write commit messages in the [Conventional Commits](https://www.conventionalcommits.org) format, for example `feat: add enqueue_at` or `fix(broker): release lease on retry`.

Report security vulnerabilities privately, as described in [SECURITY.md](SECURITY.md).

## License

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in riverbed by you, as defined in the Apache-2.0 license, shall be dual licensed as described in the [README](README.md#license), without any additional terms or conditions.
