/// Identifies which higher-level structure owns a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    Free = 0,
    BTreeLeaf = 1,
    BTreeInternal = 2,
    Heap = 3,
}

/// Slot index within a slotted page (0-based).
pub type SlotId = u16;

/// A single entry in the slot directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotEntry {
    /// Byte offset of the payload from the start of the page.
    pub offset: u16,
    /// Length of the payload in bytes.
    pub length: u16,
}

/// Fixed-size header at the start of every slotted page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    pub page_type: PageType,
    /// Number of slots in the directory (including dead slots).
    pub slot_count: u16,
    /// Byte offset where the slot directory ends (grows forward).
    pub free_space_start: u16,
    /// Byte offset where the payload area begins (grows backward).
    pub free_space_end: u16,
}
