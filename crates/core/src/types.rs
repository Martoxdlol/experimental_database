//! Core identifiers and value types shared across all layers.
//!
//! No I/O, no state — pure data definitions.

/// 128-bit ULID document identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DocId(pub [u8; 16]);

impl DocId {
    /// Access the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Monotonic collection identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CollectionId(pub u64);

/// Monotonic index identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexId(pub u64);

/// Logical timestamp for MVCC.
pub type Ts = u64;

/// Transaction identifier.
pub type TxId = u64;

/// Scalar values for index comparisons and filter evaluation.
///
/// Ordering matches TypeTag order: Undefined < Null < Int64 < Float64 < Boolean < String < Bytes < Id.
#[derive(Debug, Clone)]
pub enum Scalar {
    /// Field absent from document (distinct from null).
    Undefined,
    /// Explicit null.
    Null,
    /// 64-bit signed integer.
    Int64(i64),
    /// 64-bit IEEE 754 float.
    Float64(f64),
    /// Boolean.
    Boolean(bool),
    /// UTF-8 string.
    String(String),
    /// Raw bytes.
    Bytes(Vec<u8>),
    /// Document identifier (encodes as Crockford Base32 string).
    Id(DocId),
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Undefined, Self::Undefined) => true,
            (Self::Null, Self::Null) => true,
            (Self::Int64(a), Self::Int64(b)) => a == b,
            (Self::Float64(a), Self::Float64(b)) => a.to_bits() == b.to_bits(),
            (Self::Boolean(a), Self::Boolean(b)) => a == b,
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Bytes(a), Self::Bytes(b)) => a == b,
            (Self::Id(a), Self::Id(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Scalar {}

/// Type ordering tag for scalar values (DESIGN.md section 1.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum TypeTag {
    Undefined = 0x00,
    Null = 0x01,
    Int64 = 0x02,
    Float64 = 0x03,
    Boolean = 0x04,
    String = 0x05,
    Bytes = 0x06,
    Array = 0x07,
}

impl TypeTag {
    /// Convert from a raw byte.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x00 => Some(Self::Undefined),
            0x01 => Some(Self::Null),
            0x02 => Some(Self::Int64),
            0x03 => Some(Self::Float64),
            0x04 => Some(Self::Boolean),
            0x05 => Some(Self::String),
            0x06 => Some(Self::Bytes),
            0x07 => Some(Self::Array),
            _ => None,
        }
    }
}
