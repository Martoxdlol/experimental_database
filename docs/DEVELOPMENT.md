# Development Guide

Rules for structuring crates, modules, errors, and exports.

## Module dependency tree (no circular imports)

Within a crate, modules form a **directed acyclic graph**. A module may only
import from modules below it in the dependency tree, never above.

Example for `storage`:

```
engine          (top — facade)
├── buffer_pool
│   └── pager   (bottom — raw I/O)
└── btree
    ├── buffer_pool
    │   └── pager
    └── pager
```

- `engine` may import `buffer_pool`, `btree`, `pager`
- `buffer_pool` may import `pager`
- `btree` may import `buffer_pool` and `pager`
- `pager` imports nothing from this crate
- **`btree` must never import `engine`**
- **`buffer_pool` must never import `engine` or `btree`**
- **`pager` must never import anything above it**

The same rule applies across crates in the workspace:

```
L6 database  ->  L5 tx  ->  L4 query  ->  L3 docstore  ->  L2 storage  ->  L1 core
```

A lower layer never depends on a higher layer.

## File structure per module

Each module is a directory with these files:

```
module_name/
├── mod.rs       submodule declarations and re-exports only
├── error.rs     module's error enum (if it has one)
├── traits.rs    trait definitions (if any)
├── types.rs     type aliases and small structs (if any)
└── *.rs         implementation files
```

`mod.rs` contains **only** `pub mod` declarations. No logic, no types.

```rust
// good mod.rs
pub mod error;
pub mod traits;
pub mod types;
```

## Error definitions

Each module that can fail defines its own error enum in `error.rs`.

### Rules

1. **One error enum per module.** Named `{Module}Error` (e.g. `PagerError`,
   `BufferError`, `EngineError`).

2. **Wrap the layer below via `#[from]`**, not individual variants from it:
   ```rust
   pub enum BufferError {
       #[error(transparent)]
       Pager(#[from] PagerError),  // wraps the entire layer below
       // ... BufferError-specific variants
   }
   ```

3. **`io::Error` lives only at the lowest IO boundary.** Currently that is
   `PagerError`. No higher module re-declares an `Io` variant — IO errors
   propagate through the wrapped error chain.

4. **No redundant error paths.** If an error already reaches you through the
   `#[from]` chain, do not add a second path for it.

5. **`From` impls into external types** (e.g. `From<PagerError> for io::Error`)
   are allowed only when needed by a trait contract.

### Error propagation chain

```
io::Error  ->  PagerError::Io
                    |
               BufferError::Pager
                    |
               EngineError::Buffer
```

## Export definitions

### `lib.rs`

Top-level `lib.rs` declares `pub mod` for each module directory. It may
also re-export key types for ergonomic access:

```rust
pub mod pager;
pub mod buffer_pool;
pub mod btree;
pub mod engine;
```

### What to make `pub`

- Traits, error enums, and types that other modules or crates consume: `pub`
- Implementation details, helpers, internal structs: `pub(crate)` or private
- When in doubt, start private and widen visibility when needed

## Naming conventions

- Module directories: `snake_case`
- Error enums: `{Module}Error`
- Trait files: `traits.rs`
- Error files: `error.rs`
- Type files: `types.rs`
