# Document Store Implementation Plan — Overview

## Scope

Layer 3: Document Store & Indexing. Adds document/MVCC semantics on top of Layer 2 storage primitives. This is where domain concepts (DocId, timestamps, tombstones, version resolution) enter the system. The primary B-tree IS the document store — it is both storage and primary index (clustered).

**Depends on:** Layer 1 (Core Types) and Layer 2 (Storage Engine).
**No knowledge of:** Layer 4+ (Query Engine, Transactions, Database).

## Sub-Layer Organization (Bottom-Up Build Order)

| # | Sub-Layer | File | Dependencies | Testable Alone? |
|---|-----------|------|-------------|-----------------|
| D1 | Key Encoding | `key_encoding.rs` | L1 (types) | Yes |
| D2 | Version Resolution | `version_resolution.rs` | L1 (types) | Yes |
| D3 | Primary Index | `primary_index.rs` | D1, D2, L2 (BTreeHandle, Heap) | Yes |
| D4 | Secondary Index | `secondary_index.rs` | D1, D2, D3 | Yes |
| D5 | Array Indexing | `array_indexing.rs` | D1, L1 (encoding) | Yes |
| D6 | Index Builder | `index_builder.rs` | D3, D4, D5 | Yes (with test data) |
| D7 | Vacuum | `vacuum.rs` | D3, D4, D1 | Yes |

## Implementation Phases

### Phase A: Foundations (D1 + D2) — No L2 Dependencies
Build and fully test key encoding and version resolution independently. These are pure functions operating on types from L1.

### Phase B: Primary Index (D3) — Depends on Phase A + L2
Build the clustered primary B-tree wrapper. Requires L2 `BTreeHandle` and `Heap`. This is the core document store.

### Phase C: Secondary Index + Array Indexing (D4 + D5) — Depends on Phase B
Secondary index wraps a B-tree with MVCC version resolution and primary index verification. Array indexing computes multi-entry key sets for array fields.

### Phase D: Index Builder + Vacuum (D6 + D7) — Depends on Phase C
Background index building scans the primary index and populates a secondary index. Vacuum removes old versions from both primary and secondary indexes.

## File Map

```
docstore/
  mod.rs              — re-exports
  key_encoding.rs     — D1: order-preserving key encoding
  version_resolution.rs — D2: MVCC version resolution logic
  primary_index.rs    — D3: clustered primary B-tree wrapper
  secondary_index.rs  — D4: secondary index with version resolution
  array_indexing.rs   — D5: array index entry expansion
  index_builder.rs    — D6: background index building
  vacuum.rs           — D7: WAL-driven vacuum + rollback vacuum
```

## Dependency Direction

```
D7 (Vacuum) ──→ D4, D3, D1
D6 (IndexBuilder) ──→ D5, D4, D3
D5 (ArrayIndexing) ──→ D1
D4 (SecondaryIndex) ──→ D3, D2, D1
D3 (PrimaryIndex) ──→ D2, D1, L2
D2 (VersionResolution) ──→ L1
D1 (KeyEncoding) ──→ L1
```

No circular dependencies. Each sub-layer depends only on sub-layers below it and on L1/L2.

## Public Facade

Layer 3 does NOT have a single facade struct (unlike L2's `StorageEngine`). Instead, it exposes individual components that L4 (Query Engine), L5 (Transactions), and L6 (Database) compose:

```rust
// Key encoding (used by L4 range encoding, L5 read set intervals)
pub use key_encoding::{
    encode_scalar, decode_scalar,
    make_primary_key, parse_primary_key,
    make_secondary_key, make_secondary_key_from_prefix,
    parse_secondary_key_suffix, encode_key_prefix,
    inv_ts, prefix_successor, successor_key,
};

// Primary index (used by L4 scan/get, L5 commit)
pub use primary_index::{PrimaryIndex, PrimaryScanner, CellFlags};

// Secondary index (used by L4 index scan, L5 commit)
pub use secondary_index::{SecondaryIndex, SecondaryScanner};

// Version resolution (used internally by D3/D4)
pub use version_resolution::VersionResolver;

// Array indexing (used by L5 index delta computation)
pub use array_indexing::compute_index_entries;

// Index builder (used by L6 background task)
pub use index_builder::IndexBuilder;

// Vacuum (used by L5 commit coordinator, L6 startup)
pub use vacuum::{VacuumCoordinator, RollbackVacuum};
```

## Code Documentation Requirement

Same as Layer 2: all public structs, enums, traits, and functions must have `///` doc comments. Module-level `//!` doc comments at the top of each file.

## Key Design Decisions

1. **No circular dependencies with L2**: Layer 3 consumes `BTreeHandle`, `Heap`, `HeapRef`, `ScanIterator` from L2. It never feeds back into L2.

2. **PrimaryIndex owns inline/external decision**: The threshold-based decision of whether to store a document body inline in the B-tree leaf cell or in the external heap is made here, not in L2. L2 provides raw `BTreeHandle` and `Heap`; L3 decides how to use them.

3. **Version resolution is shared**: Both `PrimaryScanner` and `SecondaryScanner` use the same `VersionResolver` to skip invisible versions and deduplicate within a doc_id group.

4. **Secondary index entries have empty values**: The B-tree value for secondary index entries is empty (`&[]`). All information is in the key: `type_tag || encoded_value || doc_id || inv_ts`.

5. **Vacuum is WAL-driven**: Candidates come from the commit path (not a full B-tree scan). The pending queue is rebuilt from WAL on recovery.
