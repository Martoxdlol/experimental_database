#[derive(Debug, thiserror::Error)]
pub enum PageError {
    #[error("not enough free space on page ({requested} bytes requested, {available} available)")]
    OutOfSpace { requested: usize, available: usize },

    #[error("slot {0} is out of bounds")]
    SlotOutOfBounds(u16),

    #[error("slot {0} has been deleted")]
    SlotDeleted(u16),

    #[error("page buffer is too small ({0} bytes, minimum {1})")]
    BufferTooSmall(usize, usize),

    #[error("invalid page type tag: {0}")]
    InvalidPageType(u8),
}
