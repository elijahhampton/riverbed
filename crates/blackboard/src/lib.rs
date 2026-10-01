//! A runtime that coordinates durable units of work across interchangeable workers.
//!
//! Named for the blackboard architecture it is built around: workers contribute structured
//! findings, decisions and artifacts to shared durable state rather than returning prose to a
//! parent, and the runtime assembles each worker's context from that state.
//!
//! Nothing is implemented yet. See `AI_WORK_RUNTIME_SPEC.md` for the phased plan. This crate holds
//! the work store, context assembly, and worker pools, and depends on [`riverbed`] for scheduling.
