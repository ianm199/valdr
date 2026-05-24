# Source Drafts

This directory holds source-shaped Rust translations that are useful as
reference material but are not live implementation.

Rust does not compile a `src/foo.rs` file unless a crate root declares
`mod foo;` or `pub mod foo;`. Keeping unrooted files under `crates/*/src`
made bulk-translated drafts look like product code and inflated LOC-based
progress metrics. Files in this directory are intentionally parked outside the
compiled crate tree until an architecture packet chooses to integrate them.

Integration policy:

1. Move a draft into `crates/*/src` only when it is rooted by the crate root.
2. Compile it in the same commit that moves it.
3. Add or point to an objective gate: unit test, TCL runner, oracle, benchmark,
   or a documented architecture decision.
4. If a draft is only source-reading evidence, leave it here and cite it from
   the packet or architecture note.

