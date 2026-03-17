/// Opaque reference to a heap-allocated blob (page_id + slot).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapRef {
    pub(crate) page_id: u64,
    pub(crate) slot_id: u16,
}
