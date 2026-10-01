# riverbed workspace

| Crate | Description |
|---|---|
| [`riverbed`](crates/riverbed) | An embeddable and high performance distributed task queue written in Rust. |
| [`blackboard`](crates/blackboard) | A runtime that coordinates durable units of work across interchangeable workers. Unimplemented; see the spec. |

`blackboard` depends on `riverbed`. Never the reverse: nothing in `riverbed` refers to agents,
models, or tokens.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.
