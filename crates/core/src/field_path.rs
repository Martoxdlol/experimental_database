//! Field path representation for nested document fields.

/// Identifies a (possibly nested) field within a document.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldPath {
    segments: Vec<String>,
}

impl FieldPath {
    /// Create a field path from a list of segments.
    pub fn new(segments: Vec<String>) -> Self {
        Self { segments }
    }

    /// Create a single-segment field path.
    pub fn single(name: &str) -> Self {
        Self {
            segments: vec![name.to_string()],
        }
    }

    /// Access the path segments.
    pub fn segments(&self) -> &[String] {
        &self.segments
    }
}
