//! Document operations: field extraction, patch application.

use anyhow::{anyhow, Result};
use serde_json::Value;

use crate::types::{FieldPath, JsonScalar, PatchOp};

/// Apply a patch operation to an existing document (or create from scratch).
/// Returns the new JSON bytes.
pub fn apply_patch(existing_json: Option<&[u8]>, op: &PatchOp) -> Result<Vec<u8>> {
    match op {
        PatchOp::MergePatch(patch) => {
            let mut doc: Value = if let Some(bytes) = existing_json {
                serde_json::from_slice(bytes)
                    .map_err(|e| anyhow!("failed to parse existing document: {e}"))?
            } else {
                Value::Object(serde_json::Map::new())
            };
            merge_patch(&mut doc, patch);
            Ok(serde_json::to_vec(&doc)?)
        }
    }
}

/// Apply RFC 7396 merge-patch to a JSON value in place.
fn merge_patch(target: &mut Value, patch: &Value) {
    if let Value::Object(patch_map) = patch {
        // Ensure target is an object
        if !target.is_object() {
            *target = Value::Object(serde_json::Map::new());
        }
        let target_map = target.as_object_mut().unwrap();
        for (key, patch_val) in patch_map {
            if patch_val.is_null() {
                target_map.remove(key);
            } else {
                let entry = target_map.entry(key.clone()).or_insert(Value::Null);
                merge_patch(entry, patch_val);
            }
        }
    } else {
        *target = patch.clone();
    }
}

/// Extract a scalar field value from a JSON document at the given path.
pub fn extract_scalar(json: &[u8], path: &FieldPath) -> Option<JsonScalar> {
    let doc: Value = serde_json::from_slice(json).ok()?;
    let val = crate::filter::extract_field(&doc, path)?;
    crate::filter::value_to_scalar(val)
}

/// Parse JSON bytes into a serde_json Value.
pub fn parse_json(json: &[u8]) -> Result<Value> {
    serde_json::from_slice(json).map_err(|e| anyhow!("invalid JSON: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_merge_patch_basic() {
        let existing = serde_json::to_vec(&json!({"a": 1, "b": 2})).unwrap();
        let patch = json!({"b": 3, "c": 4});
        let result = apply_patch(Some(&existing), &PatchOp::MergePatch(patch)).unwrap();
        let val: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(val["a"], 1);
        assert_eq!(val["b"], 3);
        assert_eq!(val["c"], 4);
    }

    #[test]
    fn test_merge_patch_delete_key() {
        let existing = serde_json::to_vec(&json!({"a": 1, "b": 2})).unwrap();
        let patch = json!({"b": null});
        let result = apply_patch(Some(&existing), &PatchOp::MergePatch(patch)).unwrap();
        let val: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(val["a"], 1);
        assert!(val.get("b").is_none());
    }
}
