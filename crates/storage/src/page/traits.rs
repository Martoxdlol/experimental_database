use crate::page::{
    error::PageError,
    types::{PageType, SlotId},
};

/// Interface for a slotted page layout over a raw byte buffer.
///
/// ```text
/// ┌────────────┬──────────────────┬────────────┬──────────────────┐
/// │  Header    │  Slot directory  │  Free gap  │  Payload area    │
/// │            │  (grows →)       │            │  (← grows)       │
/// └────────────┴──────────────────┴────────────┴──────────────────┘
/// ```
pub trait SlottedPage {
    /// Initialize the buffer as an empty slotted page.
    fn init(&mut self, page_type: PageType) -> Result<(), PageError>;

    /// Insert a payload, returning its slot id.
    fn insert(&mut self, payload: &[u8]) -> Result<SlotId, PageError>;

    /// Read the payload for a given slot.
    fn read(&self, slot: SlotId) -> Result<&[u8], PageError>;

    /// Mark a slot as dead. Space is not reclaimed until compaction.
    fn delete(&mut self, slot: SlotId) -> Result<(), PageError>;

    /// Defragment the payload area, reclaiming dead-slot space.
    /// Slot ids remain stable.
    fn compact(&mut self);

    /// Usable free space remaining on this page.
    fn free_space(&self) -> usize;

    /// Number of slots (including dead ones).
    fn slot_count(&self) -> u16;

    /// The page type stored in the header.
    fn page_type(&self) -> Result<PageType, PageError>;
}
