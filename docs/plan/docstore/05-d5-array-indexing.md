# D5: Array Indexing

## Purpose

Handles the domain-specific rule that array fields produce one index entry per element. For a document with `tags: ["a", "b", "c"]` and an index on `tags`, this module produces 3 separate encoded key prefixes. Also handles compound indexes with at most one array field (DESIGN.md section 3.6).

## Dependencies

- **D1 (Key Encoding)**: `encode_scalar`, `encode_key_prefix`
- **L1 (`core/types.rs`)**: `Scalar`, `FieldPath`
- **L1 (`core/encoding.rs`)**: `extract_scalar`, `extract_scalars`

No L2 dependency. Pure computation on document values.

## Rust Types

```rust
use crate::core::types::{Scalar, FieldPath};
use crate::core::encoding::{extract_scalar, extract_scalars};
use crate::docstore::key_encoding::encode_scalar;

/// For a document, compute all secondary index key prefixes for a given
/// index definition.
///
/// Handles:
/// - Single field: extract scalar, produce one key prefix
/// - Single array field: extract each element, produce one key prefix per element
/// - Compound fields: concatenate scalars in field order
/// - Compound with one array field: cross-product (array elements x other field values)
///
/// Returns: Vec of encoded key prefixes. Caller appends doc_id || inv_ts to each.
///
/// Error: if more than one field in a compound index contains an array value.
pub fn compute_index_entries(
    doc: &serde_json::Value,
    field_paths: &[FieldPath],
) -> Result<Vec<Vec<u8>>>;

/// Validate that a compound index definition is legal for a given document.
/// Returns error if more than one field contains an array value.
/// (This is checked at entry computation time, but can be called separately
/// for early validation.)
pub fn validate_array_constraint(
    doc: &serde_json::Value,
    field_paths: &[FieldPath],
) -> Result<()>;
```

## Implementation Details

### compute_index_entries()

1. For each field_path in the index definition, extract the value from the document:
   - Use `extract_scalars(doc, field_path)` which returns a `Vec<Scalar>`.
   - For non-array fields, this returns a single-element vec (or empty if field is absent).
   - For array fields, this returns one element per array item.

2. Identify which field (if any) is the array field:
   - Count how many fields returned more than one scalar.
   - If more than one: return error ("compound index may contain at most one array field").

3. Generate key prefixes:

   **No array field**: single key prefix from concatenated scalars.
   ```
   let prefix = encode_key_prefix(&scalars);
   return vec![prefix];
   ```

   **One array field at position i**: for each element in the array field, produce a key prefix with the array element substituted at position i.
   ```
   for element in array_scalars {
       let mut scalars_copy = other_scalars.clone();
       scalars_copy.insert(i, element);
       prefixes.push(encode_key_prefix(&scalars_copy));
   }
   ```

4. Handle missing fields: if a field is absent from the document, use `Scalar::Undefined`. This is distinct from `Scalar::Null` (explicit null in the document).

### Missing Field Handling

Per DESIGN.md section 1.6, a missing field (undefined) is distinct from null. In the key encoding:
- Missing field → `Scalar::Undefined` → type tag `0x00`
- Explicit null → `Scalar::Null` → type tag `0x01`

If a field is missing, `extract_scalars` returns an empty vec. `compute_index_entries` maps this to `[Scalar::Undefined]` — the document still gets an index entry (so queries like "field does not exist" can use the index), but it sorts before all other values.

### Compound Index Example

Index on `[status, tags]` where `tags` is an array:

```json
{ "status": "active", "tags": ["rust", "db", "mvcc"] }
```

Produces 3 key prefixes:
1. `encode(String("active")) || encode(String("rust"))`
2. `encode(String("active")) || encode(String("db"))`
3. `encode(String("active")) || encode(String("mvcc"))`

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| Multiple array fields | Compound index with > 1 array field | Return error |

## Tests

1. **Single field, scalar**: doc `{ "name": "Alice" }`, index on `["name"]` → 1 key prefix.
2. **Single field, missing**: doc `{}`, index on `["name"]` → 1 key prefix with undefined tag.
3. **Single field, null**: doc `{ "name": null }`, index on `["name"]` → 1 key prefix with null tag.
4. **Single field, array**: doc `{ "tags": ["a", "b", "c"] }`, index on `["tags"]` → 3 key prefixes.
5. **Single field, empty array**: doc `{ "tags": [] }`, index on `["tags"]` → 0 key prefixes (no entries to index).
6. **Compound, no array**: doc `{ "a": 1, "b": "x" }`, index on `["a", "b"]` → 1 key prefix.
7. **Compound, one array**: doc `{ "a": 1, "tags": ["x", "y"] }`, index on `["a", "tags"]` → 2 key prefixes, each with `a=1` prefix.
8. **Compound, array first**: doc `{ "tags": ["x", "y"], "b": 1 }`, index on `["tags", "b"]` → 2 key prefixes, each with `b=1` suffix.
9. **Compound, two arrays**: doc `{ "a": [1, 2], "b": [3, 4] }`, index on `["a", "b"]` → error.
10. **Nested field**: doc `{ "user": { "email": "a@b.c" } }`, index on `[["user", "email"]]` → 1 key prefix.
11. **Key prefix encoding**: verify the output matches manually encoded `encode_scalar` concatenation.
12. **Validate array constraint**: verify `validate_array_constraint` returns Ok for valid and Err for invalid.
13. **Large array**: doc with 100-element array → 100 key prefixes.
14. **Mixed types in array**: doc `{ "vals": [1, "two", null] }` → 3 key prefixes with different type tags.
