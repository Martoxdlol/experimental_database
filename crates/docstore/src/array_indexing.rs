//! D5: Array Indexing — multi-entry key expansion for array fields.
//!
//! For a document with `tags: ["a", "b", "c"]` and an index on `tags`,
//! produces 3 separate encoded key prefixes. Handles compound indexes
//! with at most one array field.

use crate::key_encoding::encode_key_prefix;
use exdb_core::encoding::extract_scalars;
use exdb_core::field_path::FieldPath;
use exdb_core::types::Scalar;

/// For a document, compute all secondary index key prefixes for a given index definition.
///
/// Returns: `Vec` of encoded key prefixes. Caller appends `doc_id || inv_ts` to each.
///
/// Error: if more than one field in a compound index contains an array value.
pub fn compute_index_entries(
    doc: &serde_json::Value,
    field_paths: &[FieldPath],
) -> Result<Vec<Vec<u8>>, String> {
    // Extract scalars for each field
    let mut field_values: Vec<Vec<Scalar>> = Vec::with_capacity(field_paths.len());
    let mut array_field_idx: Option<usize> = None;

    for (i, path) in field_paths.iter().enumerate() {
        let scalars = extract_scalars(doc, path);
        if scalars.len() > 1 {
            // Array field
            if array_field_idx.is_some() {
                return Err("compound index may contain at most one array field".into());
            }
            array_field_idx = Some(i);
        }
        field_values.push(scalars);
    }

    // Handle missing fields: map empty to [Scalar::Undefined]
    for values in &mut field_values {
        if values.is_empty() {
            values.push(Scalar::Undefined);
        }
    }

    match array_field_idx {
        None => {
            // No array field: single key prefix from concatenated scalars
            let scalars: Vec<Scalar> = field_values.into_iter().map(|mut v| v.remove(0)).collect();
            Ok(vec![encode_key_prefix(&scalars)])
        }
        Some(arr_idx) => {
            let array_values = &field_values[arr_idx];
            if array_values.is_empty() || (array_values.len() == 1 && matches!(array_values[0], Scalar::Undefined)) {
                // Empty array after undefined fallback — produce one entry with undefined
                let scalars: Vec<Scalar> = field_values.into_iter().map(|mut v| v.remove(0)).collect();
                return Ok(vec![encode_key_prefix(&scalars)]);
            }

            // Check for truly empty arrays (extract_scalars returns empty for [])
            // which we already handled above by converting to [Undefined]

            let mut prefixes = Vec::with_capacity(array_values.len());
            for element in array_values {
                let mut scalars = Vec::with_capacity(field_paths.len());
                for (i, fv) in field_values.iter().enumerate() {
                    if i == arr_idx {
                        scalars.push(element.clone());
                    } else {
                        scalars.push(fv[0].clone());
                    }
                }
                prefixes.push(encode_key_prefix(&scalars));
            }
            Ok(prefixes)
        }
    }
}

/// Validate that a compound index definition is legal for a given document.
pub fn validate_array_constraint(
    doc: &serde_json::Value,
    field_paths: &[FieldPath],
) -> Result<(), String> {
    let mut array_count = 0;
    for path in field_paths {
        let scalars = extract_scalars(doc, path);
        if scalars.len() > 1 {
            array_count += 1;
        }
    }
    if array_count > 1 {
        Err("compound index may contain at most one array field".into())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_encoding::encode_scalar;
    use serde_json::json;

    #[test]
    fn single_field_scalar() {
        let doc = json!({"name": "Alice"});
        let paths = [FieldPath::single("name")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn single_field_missing() {
        let doc = json!({});
        let paths = [FieldPath::single("name")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 1);
        // Should start with undefined tag 0x00
        assert_eq!(entries[0][0], 0x00);
    }

    #[test]
    fn single_field_null() {
        let doc = json!({"name": null});
        let paths = [FieldPath::single("name")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 1);
        // Should start with null tag 0x01
        assert_eq!(entries[0][0], 0x01);
    }

    #[test]
    fn single_field_array() {
        let doc = json!({"tags": ["a", "b", "c"]});
        let paths = [FieldPath::single("tags")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn single_field_empty_array() {
        let doc = json!({"tags": []});
        let paths = [FieldPath::single("tags")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        // Empty array -> extract_scalars returns empty -> we map to [Undefined]
        // So we get 1 entry with undefined
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn compound_no_array() {
        let doc = json!({"a": 1, "b": "x"});
        let paths = [FieldPath::single("a"), FieldPath::single("b")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn compound_one_array() {
        let doc = json!({"a": 1, "tags": ["x", "y"]});
        let paths = [FieldPath::single("a"), FieldPath::single("tags")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 2);
        // Each entry should have a=1 prefix
    }

    #[test]
    fn compound_array_first() {
        let doc = json!({"tags": ["x", "y"], "b": 1});
        let paths = [FieldPath::single("tags"), FieldPath::single("b")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn compound_two_arrays_error() {
        let doc = json!({"a": [1, 2], "b": [3, 4]});
        let paths = [FieldPath::single("a"), FieldPath::single("b")];
        assert!(compute_index_entries(&doc, &paths).is_err());
    }

    #[test]
    fn nested_field() {
        let doc = json!({"user": {"email": "a@b.c"}});
        let paths = [FieldPath::new(vec!["user".into(), "email".into()])];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn key_prefix_matches_manual() {
        let doc = json!({"name": "Alice"});
        let paths = [FieldPath::single("name")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        let expected = encode_scalar(&Scalar::String("Alice".into()));
        assert_eq!(entries[0], expected);
    }

    #[test]
    fn validate_array_constraint_ok() {
        let doc = json!({"a": 1, "tags": ["x", "y"]});
        let paths = [FieldPath::single("a"), FieldPath::single("tags")];
        assert!(validate_array_constraint(&doc, &paths).is_ok());
    }

    #[test]
    fn validate_array_constraint_err() {
        let doc = json!({"a": [1, 2], "b": [3, 4]});
        let paths = [FieldPath::single("a"), FieldPath::single("b")];
        assert!(validate_array_constraint(&doc, &paths).is_err());
    }

    #[test]
    fn large_array() {
        let arr: Vec<i64> = (0..100).collect();
        let doc = json!({"vals": arr});
        let paths = [FieldPath::single("vals")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 100);
    }

    #[test]
    fn mixed_types_in_array() {
        let doc = json!({"vals": [1, "two", null]});
        let paths = [FieldPath::single("vals")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 3);
        // Different type tags
        assert_ne!(entries[0][0], entries[1][0]);
    }

    // ─── Additional edge case tests ───

    #[test]
    fn single_element_array() {
        let doc = json!({"tags": ["only"]});
        let paths = [FieldPath::single("tags")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 1);
        let expected = encode_scalar(&Scalar::String("only".into()));
        assert_eq!(entries[0], expected);
    }

    #[test]
    fn deeply_nested_field() {
        let doc = json!({"a": {"b": {"c": "deep"}}});
        let paths = [FieldPath::new(vec!["a".into(), "b".into(), "c".into()])];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 1);
        let expected = encode_scalar(&Scalar::String("deep".into()));
        assert_eq!(entries[0], expected);
    }

    #[test]
    fn deeply_nested_missing() {
        let doc = json!({"a": {"b": {}}});
        let paths = [FieldPath::new(vec!["a".into(), "b".into(), "c".into()])];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 1);
        // Missing → Undefined
        assert_eq!(entries[0][0], 0x00);
    }

    #[test]
    fn compound_with_missing_and_array() {
        // First field missing, second field is array
        let doc = json!({"tags": ["x", "y"]});
        let paths = [FieldPath::single("name"), FieldPath::single("tags")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 2);
        // Each should start with undefined tag (0x00) for missing name
        for entry in &entries {
            assert_eq!(entry[0], 0x00);
        }
    }

    #[test]
    fn array_of_null_values() {
        let doc = json!({"vals": [null, null, null]});
        let paths = [FieldPath::single("vals")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 3);
        // All should have null tag
        for entry in &entries {
            assert_eq!(entry[0], 0x01);
        }
    }

    #[test]
    fn array_of_booleans() {
        let doc = json!({"flags": [true, false, true]});
        let paths = [FieldPath::single("flags")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn array_of_numbers() {
        let doc = json!({"nums": [1, 2, 3, -1, 0]});
        let paths = [FieldPath::single("nums")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 5);
        // Entries should be in order-preserving encoding — verify ordering
        // -1 < 0 < 1 < 2 < 3 in encoded form
        let sorted: Vec<_> = {
            let mut e = entries.clone();
            e.sort();
            e
        };
        // The entries corresponding to -1, 0, 1, 2, 3 should sort correctly
        assert!(sorted[0] < sorted[1]); // -1 < 0
        assert!(sorted[1] < sorted[2]); // 0 < 1
    }

    #[test]
    fn compound_three_fields_one_array() {
        let doc = json!({"a": 1, "b": ["x", "y"], "c": true});
        let paths = [
            FieldPath::single("a"),
            FieldPath::single("b"),
            FieldPath::single("c"),
        ];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 2); // 2 array elements
    }

    #[test]
    fn compound_three_fields_no_array() {
        let doc = json!({"a": 1, "b": "hello", "c": true});
        let paths = [
            FieldPath::single("a"),
            FieldPath::single("b"),
            FieldPath::single("c"),
        ];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn nested_array_field_in_compound() {
        let doc = json!({"user": {"tags": ["a", "b"]}});
        let paths = [FieldPath::new(vec!["user".into(), "tags".into()])];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn object_field_becomes_undefined() {
        // Objects don't map to scalars
        let doc = json!({"nested": {"a": 1}});
        let paths = [FieldPath::single("nested")];
        let entries = compute_index_entries(&doc, &paths).unwrap();
        assert_eq!(entries.len(), 1);
        // Object → Undefined via json_to_scalar
        assert_eq!(entries[0][0], 0x00);
    }

    #[test]
    fn validate_single_array_ok() {
        let doc = json!({"a": 1, "b": [1, 2]});
        let paths = [FieldPath::single("a"), FieldPath::single("b")];
        assert!(validate_array_constraint(&doc, &paths).is_ok());
    }

    #[test]
    fn validate_no_arrays_ok() {
        let doc = json!({"a": 1, "b": 2});
        let paths = [FieldPath::single("a"), FieldPath::single("b")];
        assert!(validate_array_constraint(&doc, &paths).is_ok());
    }
}
