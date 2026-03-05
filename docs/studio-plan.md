# exdb-studio: Database Inspector & Editor (Dioxus Desktop App)

A pure-Rust desktop application built with **Dioxus 0.7** for inspecting, browsing,
and **mutating** exdb databases at every level of the stack. Opens database files
directly and exposes the raw storage internals: pages, slots, B-trees, heap chains,
WAL segments, free list, and buffer pool state — with full read-write access from
high-level document operations down to raw page byte editing.

---

## 1. Architecture

### 1.1 Layered Design

The studio follows a strict three-layer separation:

```
┌─────────────────────────────────────────────────────┐
│  UI Layer (Dioxus components + CSS)                 │
│  Pure rendering. No storage access.                 │
├─────────────────────────────────────────────────────┤
│  ViewModel Layer (signals + async commands)          │
│  Translates storage data → display models.          │
│  Runs all I/O on tokio::spawn_blocking.             │
├─────────────────────────────────────────────────────┤
│  Inspector Layer (read-only storage access)          │
│  Wraps exdb-storage. Reads pages, WAL, B-trees.    │
│  Never mutates. Opens database in read-only mode.   │
└─────────────────────────────────────────────────────┘
```

### 1.2 Module System

The app is organized into **modules** — self-contained feature areas, each with its own
panel, routes, and view models. Modules can be loaded independently.

```
studio/
├── src/
│   ├── main.rs                 # Dioxus launch + top-level router
│   ├── app.rs                  # Root App component (layout shell)
│   ├── theme.rs                # Colors, spacing, typography tokens
│   ├── state.rs                # Global app state (open db, active module, rw lock)
│   │
│   ├── engine/                 # Engine layer (full read-write storage access)
│   │   ├── mod.rs
│   │   ├── db_handle.rs        # Open/close database, hold StorageEngine + handles
│   │   ├── page_reader.rs      # Read & decode any page by ID
│   │   ├── page_writer.rs      # Raw page byte writes, slot insert/update/delete
│   │   ├── wal_reader.rs       # Iterate WAL segments and frames
│   │   ├── wal_writer.rs       # Append WAL records (any record type)
│   │   ├── btree_reader.rs     # Walk B-tree structure
│   │   ├── btree_writer.rs     # B-tree insert/delete key-value pairs
│   │   ├── heap_reader.rs      # Follow heap refs and overflow chains
│   │   ├── heap_writer.rs      # Store/free heap blobs
│   │   ├── free_list_reader.rs # Walk the free page chain
│   │   ├── free_list_writer.rs # Allocate/deallocate pages
│   │   ├── catalog_reader.rs   # Read catalog B-trees (collections, indexes)
│   │   ├── catalog_writer.rs   # Create/drop collections and indexes via catalog
│   │   ├── checkpoint.rs       # Trigger checkpoint, inspect dirty pages
│   │   └── header_writer.rs    # Read/update FileHeader fields directly
│   │
│   ├── modules/                # UI modules (one per feature area)
│   │   ├── mod.rs              # Module registry
│   │   ├── overview/           # Database overview / dashboard + header editor
│   │   ├── collections/        # High-level: collection & document CRUD
│   │   ├── pages/              # Page explorer + raw page editor
│   │   ├── btree/              # B-tree visualizer + key-value editor
│   │   ├── wal/                # WAL segment browser + record injector
│   │   ├── heap/               # Heap viewer + blob store/free
│   │   ├── freelist/           # Free list inspector + manual alloc/dealloc
│   │   ├── catalog/            # Catalog browser + collection/index management
│   │   └── console/            # Raw engine API console
│   │
│   └── components/             # Shared UI components
│       ├── mod.rs
│       ├── hex_editor.rs       # Hex dump with inline byte editing + highlighting
│       ├── page_header.rs      # Page header display + editable fields
│       ├── slot_table.rs       # Slot directory table + insert/delete actions
│       ├── key_value_table.rs  # Generic key-value display + inline edit
│       ├── json_editor.rs      # JSON viewer/editor for document values
│       ├── confirm_dialog.rs   # Confirmation modal for destructive operations
│       ├── rw_lock_badge.rs    # Read-only / read-write lock toggle widget
│       ├── breadcrumb.rs       # Navigation breadcrumb
│       ├── sidebar.rs          # Module navigation sidebar
│       ├── toolbar.rs          # Top toolbar (file open, db info, rw lock)
│       └── byte_bar.rs         # Visual page space usage bar
```

### 1.3 Crate Dependencies

```toml
[package]
name = "exdb-studio"
version.workspace = true
edition.workspace = true

[dependencies]
exdb-storage = { workspace = true }
dioxus = { version = "0.7", features = ["desktop"] }
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
rfd = "0.15"                    # Native file dialog
```

No web server. No JavaScript. Pure Rust desktop app.

---

## 2. General Layout

### 2.1 Window Structure

```
┌─────────────────────────────────────────────────────────────────┐
│  Toolbar                                                         │
│  [Open DB...] [Close]  │  data.db (8192 B pages, 147 pages)    │
│  [Checkpoint] [Dirty: 3]                        🔒 READ-ONLY    │
├────────┬────────────────────────────────────────────────────────┤
│        │  Breadcrumb: Database > Pages > Page #42               │
│  Side  │────────────────────────────────────────────────────────│
│  bar   │                                                        │
│        │  Main Content Area                                     │
│  ○ Ovw │                                                        │
│  ○ Cols│  (changes per module — page detail, B-tree viz,       │
│  ● Pgs │   WAL frames, hex dump, document editor, etc.)        │
│  ○ BTree│                                                       │
│  ○ WAL │                                                        │
│  ○ Heap│                                                        │
│  ○ Free│                                                        │
│  ○ Cat │                                                        │
│  ○ Cons│                                                        │
│        ├────────────────────────────────────────────────────────│
│        │  Detail / Editor Panel (collapsible)                   │
│        │  Hex editor, raw bytes, slot editor, JSON editor       │
└────────┴────────────────────────────────────────────────────────┘
```

### 2.2 Layout Components

- **Toolbar**: File operations (open via `rfd` native dialog), database summary info.
  Shows: file path, page size, page count, checkpoint LSN, creation date.
  Also: **Checkpoint** button (triggers manual checkpoint), **Dirty page count** badge,
  and **RW Lock toggle** (🔒 READ-ONLY / 🔓 READ-WRITE).
- **Sidebar**: Module navigation. Icons + labels. Highlights active module.
  Disabled when no database is open. New entries: **Cols** (Collections) and **Cons** (Console).
- **Breadcrumb**: Contextual path within the active module (e.g., `Pages > #42 > Slot 3`).
- **Main Content**: Module-specific content. Each module owns this area entirely.
  Write actions appear inline (edit buttons, insert forms) but are disabled when locked.
- **Detail Panel**: Bottom split panel for deep inspection **and editing** (hex editor,
  JSON editor, slot editor). Toggled per module. Resizable.

### 2.3 Interaction Model

1. User clicks **Open DB...** → native file dialog → select database directory
2. Engine layer opens `StorageEngine::open` in full read-write mode (WAL recovery runs)
3. RW lock defaults to **locked** (🔒) — all write UI is grayed out
4. Sidebar modules become active
5. User navigates modules; each module queries the engine layer
6. All storage I/O happens on `spawn_blocking` to keep UI responsive
7. To write: user clicks **🔒 → 🔓** toggle, confirms in dialog ("Enable writes?")
8. Write actions become available: inline edit buttons, insert forms, action bars
9. Every destructive write (delete slot, drop collection, raw page overwrite) shows
   a **confirmation dialog** with a summary of what will change
10. User can trigger **Checkpoint** at any time to flush dirty pages to disk

---

## 3. Modules

### 3.1 Overview Module

**Purpose**: Dashboard showing database-level statistics, health, and **direct
FileHeader editing**.

**Content**:
```
┌─────────────────────────────────────────────────────┐
│  Database Overview                                   │
│                                                      │
│  File Header                               [Edit ✎] │
│  ┌────────────────────────────────────────────────┐ │
│  │ Magic:          EXDB (0x45584442)              │ │
│  │ Version:        1                               │ │
│  │ Page Size:      8,192 bytes                     │ │
│  │ Page Count:     147                    [✎]      │ │
│  │ Free List Head: Page #12               [✎]      │ │
│  │ Catalog Root:   Page #1                [✎]      │ │
│  │ Name Root:      Page #2                [✎]      │ │
│  │ Checkpoint LSN: 4,096                  [✎]      │ │
│  │ Visible TS:     1,709,234,567          [✎]      │ │
│  │ Generation:     3                      [✎]      │ │
│  │ Created:        2026-03-05 14:30:00 UTC         │ │
│  └────────────────────────────────────────────────┘ │
│                                                      │
│  Space Usage                                         │
│  ┌────────────────────────────────────────────────┐ │
│  │ ███████░░░░░░░░ BTree Internal (12 pages)      │ │
│  │ ██████████████░ BTree Leaf     (48 pages)      │ │
│  │ ████░░░░░░░░░░░ Heap           (8 pages)       │ │
│  │ ██░░░░░░░░░░░░░ Overflow       (3 pages)       │ │
│  │ █░░░░░░░░░░░░░░ Free           (2 pages)       │ │
│  │ █░░░░░░░░░░░░░░ FileHeader     (1 page)        │ │
│  └────────────────────────────────────────────────┘ │
│                                                      │
│  WAL Summary                                         │
│  ┌────────────────────────────────────────────────┐ │
│  │ Segments:       3                               │ │
│  │ Oldest LSN:     0                               │ │
│  │ Current LSN:    12,288                          │ │
│  │ Total Size:     384 KB                          │ │
│  └────────────────────────────────────────────────┘ │
│                                                      │
│  Actions                                             │
│  ┌────────────────────────────────────────────────┐ │
│  │ [Checkpoint Now]  [Vacuum]  [Bump Generation]  │ │
│  └────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

**Data sources**: `FileHeader`, page scan (count by type), WAL metadata.

**Write features** (require 🔓 unlocked):
- **Edit FileHeader fields**: Click [✎] next to any mutable field → inline edit →
  calls `StorageEngine::update_file_header`. Editable fields: `page_count`,
  `free_list_head`, `catalog_root_page`, `catalog_name_root_page`, `checkpoint_lsn`,
  `visible_ts`, `generation`, `next_collection_id`, `next_index_id`.
- **Checkpoint Now**: Trigger `StorageEngine::checkpoint()` to flush dirty pages.
- **Vacuum**: Trigger vacuum on dead entries (shows entry count preview first).
- **Bump Generation**: Increment generation counter (useful for replication).

### 3.2 Pages Module (Core)

**Purpose**: Browse every page in the database. The primary inspection tool.

**Layout**: Master-detail with page list on the left, page detail on the right.

```
┌──────────────────┬──────────────────────────────────────────┐
│  Page List        │  Page #42 — BTreeLeaf                   │
│                   │                                          │
│  Filter: [____]   │  Header                                  │
│  Type: [All    ▾] │  ┌──────────────────────────────────┐   │
│                   │  │ page_id:          42              │   │
│  #0  FileHeader   │  │ page_type:        BTreeLeaf(0x02)│   │
│  #1  BTreeLeaf    │  │ flags:            0x00            │   │
│  #2  BTreeLeaf    │  │ num_slots:        23              │   │
│  #3  BTreeInternal│  │ free_space_start: 124             │   │
│  ...              │  │ free_space_end:   6,847           │   │
│  #42 BTreeLeaf  ◄ │  │ prev_or_ptr:      43 (→ click)   │   │
│  #43 BTreeLeaf    │  │ checksum:         0xA3B2C1D0  ✓  │   │
│  ...              │  │ lsn:              8,192           │   │
│                   │  └──────────────────────────────────┘   │
│                   │                                          │
│                   │  Space Map                               │
│                   │  ┌──────────────────────────────────┐   │
│                   │  │ [HDR][S0][S1]...[S22][ free ]DATA│   │
│                   │  │  32   92 bytes dir  │ 6723  │cells│   │
│                   │  └──────────────────────────────────┘   │
│                   │                                          │
│                   │  Slot Directory                          │
│                   │  ┌───┬────────┬────────┬────────────┐   │
│                   │  │ # │ Offset │ Length │ Preview     │   │
│                   │  ├───┼────────┼────────┼────────────┤   │
│                   │  │ 0 │ 8,160  │ 32     │ 00 0A 4B.. │   │
│                   │  │ 1 │ 8,128  │ 28     │ 00 08 6D.. │   │
│                   │  │ 2 │ 0      │ 0      │ (deleted)  │   │
│                   │  │ …                                │   │
│                   │  └──────────────────────────────────┘   │
│                   │                                          │
│                   │  ┌──────────────────────────────────┐   │
│                   │  │ Hex Dump (bottom panel)           │   │
│                   │  │ 00000000: 2A00 0000 0205 1700  │   │
│                   │  │ 00000008: 7C00 BF1A 2B00 0000  │   │
│                   │  │ ...                               │   │
│                   │  └──────────────────────────────────┘   │
└──────────────────┴──────────────────────────────────────────┘
```

**Read features**:
- **Page list**: Scrollable list of all pages, colored by type, filterable.
- **Page header**: Decoded `PageHeader` fields with validation (checksum ✓/✗).
- **Space map**: Visual bar showing header, slot directory, free space, and cell regions.
  Color-coded. Shows exact byte ranges.
- **Slot directory**: Table of all slots with offset, length, and data preview.
  Clicking a slot highlights it in the hex dump and space map.
  Tombstones (length=0) shown as "(deleted)".
- **Hex dump**: Full page hex view with ASCII sidebar. Regions highlighted by color
  (header=blue, slots=green, free=gray, cells=orange). Selected slot highlighted.
- **Navigation**: `prev_or_ptr` is clickable → navigate to linked page.

**Type-specific decoding**:
- `BTreeLeaf`: Decode cells as `key_len(u16) | key | value`. Show key/value pairs.
- `BTreeInternal`: Decode cells as `key_len(u16) | key | child_page(u32)`.
  Child pages are clickable links.
- `Heap`: Decode slot as `flags(u8) | total_len(u32) | overflow_page(u32) | data`.
  Overflow page is clickable.
- `Overflow`: Show `data_len(u32) | data`. Next page (`prev_or_ptr`) is clickable.
- `Free`: Show next-free pointer (`prev_or_ptr`), clickable.
- `FileHeader`: Decode `FileHeader` struct fields.

**Write features** (require 🔓 unlocked):
- **Hex editor**: Click any byte in the hex dump → edit in place. Modified bytes
  highlighted in yellow. [Apply] writes the raw page buffer via
  `BufferPool::fetch_page_exclusive` + `data_mut()` + `mark_dirty()`.
- **Edit page header fields**: Click [✎] next to any header field (page_type,
  flags, prev_or_ptr, lsn) → inline numeric input → writes via `SlottedPage::set_header`.
- **Insert slot**: [+ Insert Slot] button below slot directory → hex input for cell
  data → calls `SlottedPage::insert_slot`.
- **Update slot**: Click [✎] on a slot row → hex editor for slot data →
  calls `SlottedPage::update_slot`.
- **Delete slot**: Click [✗] on a slot row → confirmation → calls
  `SlottedPage::delete_slot` (marks as tombstone).
- **Compact page**: [Compact] button → calls `SlottedPage::compact` to defragment.
- **Re-stamp checksum**: [Checksum] button → calls `SlottedPage::stamp_checksum`.
- **Initialize page**: Right-click a page in the list → "Re-initialize as..." →
  pick page type → calls `SlottedPage::init` (⚠ destructive, confirmation required).

### 3.3 B-Tree Module

**Purpose**: Visualize B-tree structure as a navigable tree.

**Layout**:

```
┌─────────────────────────────────────────────────────────────┐
│  B-Tree Explorer                                             │
│                                                              │
│  Select tree: [Catalog by ID ▾]  Root: Page #1              │
│                                                              │
│  Tree View (expandable)                                      │
│  ┌─────────────────────────────────────────────────────────┐│
│  │  [Internal #1] (3 keys, 4 children)                     ││
│  │  ├── child₀ → [Leaf #3] (15 entries)                    ││
│  │  │   ├── key: 00 01 ... → val: 28 bytes                 ││
│  │  │   ├── key: 00 02 ... → val: 32 bytes                 ││
│  │  │   └── ...                                             ││
│  │  ├── key₁: 00 0A 4B ...                                 ││
│  │  ├── child₁ → [Leaf #5] (12 entries)                    ││
│  │  ├── key₂: 00 14 ...                                    ││
│  │  ├── child₂ → [Leaf #7] (18 entries)                    ││
│  │  ├── key₃: 00 1E ...                                    ││
│  │  └── child₃ → [Leaf #9] (10 entries)                    ││
│  └─────────────────────────────────────────────────────────┘│
│                                                              │
│  Selected: Leaf #5                                           │
│  ┌─────────────────────────────────────────────────────────┐│
│  │ Page detail (same as Pages module detail view)           ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

**Read features**:
- Dropdown to select which B-tree to explore (catalog by-id, catalog by-name,
  or any collection/index B-tree discovered via catalog).
- Tree view with expand/collapse. Each node shows page ID, type, key count.
- Leaf nodes expandable to show individual key-value entries.
- Clicking any node opens its page detail (reuses Pages module components).
- Keys displayed as hex with optional decoded interpretation.
- **Open by root page**: Manual input to open any B-tree by its root page ID.

**Write features** (require 🔓 unlocked):
- **Insert key-value**: [+ Insert] button → key (hex) + value (hex or JSON) input →
  calls `BTreeHandle::insert`. The tree view auto-refreshes after insert.
- **Delete key**: Right-click any leaf entry → "Delete key" → confirmation →
  calls `BTreeHandle::delete`. Shows the key being deleted.
- **Create new B-tree**: [New B-Tree] button → allocates a fresh empty B-tree via
  `StorageEngine::create_btree`. Returns the new root page ID.
- **Bulk insert**: [Import...] → paste or load a list of hex key-value pairs
  (one per line) → batch `BTreeHandle::insert`.

### 3.4 WAL Module

**Purpose**: Browse WAL segments and individual frames.

```
┌──────────────────┬──────────────────────────────────────────┐
│  WAL Segments     │  Frame Detail                            │
│                   │                                          │
│  segment-000001   │  Frame @ LSN 4,096                       │
│    32 B header    │  ┌──────────────────────────────────┐   │
│    2,847 frames   │  │ LSN:           4,096             │   │
│    64.0 MB        │  │ Payload Len:   128               │   │
│                   │  │ CRC-32C:       0xB2A1... ✓       │   │
│  segment-000002   │  │ Record Type:   0x01 (TX_COMMIT)  │   │
│    32 B header    │  └──────────────────────────────────┘   │
│    1,203 frames   │                                          │
│    27.3 MB        │  Payload Hex                             │
│                   │  ┌──────────────────────────────────┐   │
│ ──────────────── │  │ 00000000: 0102 0340 0000 ...     │   │
│  Frames (seg 1)   │  │ 00000008: 4865 6C6C 6F20 ...     │   │
│                   │  │ ...                               │   │
│  LSN 0     0x01   │  └──────────────────────────────────┘   │
│  LSN 137   0x01   │                                          │
│  LSN 274   0x03 ◄ │                                          │
│  LSN 4096  0x01   │                                          │
│  ...              │                                          │
└──────────────────┴──────────────────────────────────────────┘
```

**Read features**:
- List WAL segments with header info (magic, version, base_lsn, segment_id).
- List all frames within a segment, showing LSN, record type, payload size.
- Record type decoded to human name (TX_COMMIT, CHECKPOINT, CREATE_COLLECTION, etc.).
- CRC validation per frame (✓/✗).
- Payload hex dump with highlighting.
- Filter by record type.

**Write features** (require 🔓 unlocked):
- **Append WAL record**: [+ Append Record] → select record type from dropdown
  (TX_COMMIT, CHECKPOINT, CREATE_COLLECTION, DROP_COLLECTION, CREATE_INDEX,
  DROP_INDEX, INDEX_READY, VACUUM, VISIBLE_TS, ROLLBACK_VACUUM) → hex editor for
  payload bytes → calls `StorageEngine::append_wal`. Shows resulting LSN.
- **Append raw frame**: [+ Raw Frame] → hex editor for the complete pre-encoded
  frame bytes → calls `WalWriter::append_raw_frame`.
- **Structured record builders**: For common record types, show a form instead of
  raw hex. E.g., CREATE_COLLECTION shows fields for collection_id + name;
  TX_COMMIT shows fields for tx_id + entries. The form serializes to the correct
  payload format.
- **Truncate WAL**: [Truncate Before...] → input LSN → confirmation dialog →
  calls `WalStorage::truncate_before` (⚠ destructive).

### 3.5 Heap Module

**Purpose**: Inspect heap pages and follow overflow chains.

```
┌─────────────────────────────────────────────────────────────┐
│  Heap Inspector                                              │
│                                                              │
│  Heap Pages: 8 total                                         │
│  ┌────┬───────┬───────┬────────────────────────────────┐    │
│  │ Pg │ Slots │ Free  │ Usage                          │    │
│  ├────┼───────┼───────┼────────────────────────────────┤    │
│  │ 20 │ 3     │ 1,204 │ ██████████████░░░░ 85%         │    │
│  │ 21 │ 1     │ 7,800 │ ███░░░░░░░░░░░░░░░ 5%          │    │
│  │ 24 │ 5     │ 0     │ ██████████████████ 100%         │    │
│  └────┴───────┴───────┴────────────────────────────────┘    │
│                                                              │
│  Selected: HeapRef { page: 20, slot: 1 }                     │
│  ┌─────────────────────────────────────────────────────────┐│
│  │ Flags:         0x01 (HAS_OVERFLOW)                      ││
│  │ Total Length:  24,500 bytes                              ││
│  │ First Chunk:   8,147 bytes (on this page)               ││
│  │ Overflow:      Page #30 → Page #31 → Page #32 (end)     ││
│  │                                                          ││
│  │ Assembled Data (first 256 bytes):                        ││
│  │ 7B 22 6E 61 6D 65 22 3A ...  {"name":...               ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

**Read features**:
- List all heap-type pages with slot count and space usage.
- Select a heap slot → decode header (flags, total_length, overflow_page).
- Visualize overflow chain as clickable linked list.
- Reassemble and display full blob (with truncation for large values).
- Show data as hex + attempt JSON decode if valid UTF-8.

**Write features** (require 🔓 unlocked):
- **Store blob**: [+ Store Blob] → hex editor or JSON editor for data →
  calls `StorageEngine::heap_store`. Shows the returned `HeapRef` (page + slot).
  Automatically allocates heap pages and overflow chains for large values.
- **Free blob**: Select a heap slot → [Free] button → confirmation showing the
  overflow chain that will be reclaimed → calls `StorageEngine::heap_free`.
  Pages are returned to the free list.
- **Edit blob inline**: Select a heap slot → [Edit] → JSON or hex editor for the
  assembled blob data → free old + store new (since heap blobs are immutable,
  editing is a free-then-store operation; the HeapRef will change).

### 3.6 Free List Module

**Purpose**: Walk the free page chain.

```
┌─────────────────────────────────────────────────────────────┐
│  Free List                                                   │
│                                                              │
│  Head: Page #12                                              │
│  Length: 5 pages                                             │
│                                                              │
│  Chain:                                                      │
│  Page #12 → Page #15 → Page #18 → Page #22 → Page #25 → ∅  │
│                                                              │
│  (each page ID is clickable → opens in Pages module)         │
└─────────────────────────────────────────────────────────────┘
```

Simple chain visualization. Each page clickable to inspect in Pages module.

**Write features** (require 🔓 unlocked):
- **Allocate page**: [Allocate] button → pops a page from the free list head (or
  extends the file) via `FreeList::allocate`. Shows the newly allocated page ID.
  The page is initialized as the selected type (default: BTreeLeaf).
- **Deallocate page**: [+ Deallocate] → input page ID → confirmation showing current
  page type and contents summary → calls `FreeList::deallocate`. Pushes page onto
  free list head. (⚠ Does not clean up references to this page in B-trees or heap.)
- **Bulk allocate**: [Allocate N...] → input count → allocates N pages in sequence.
  Returns list of allocated page IDs.

### 3.7 Catalog Module

**Purpose**: Browse the catalog B-trees to see collections and indexes.

```
┌─────────────────────────────────────────────────────────────┐
│  Catalog Browser                                             │
│                                                              │
│  Collections (by-ID tree, root: Page #1)                     │
│  ┌─────┬──────────────┬────────────────────────────────┐    │
│  │ ID  │ Name          │ Data Root   │ Index Count      │    │
│  ├─────┼──────────────┼────────────────────────────────┤    │
│  │ 1   │ users         │ Page #10    │ 2                │    │
│  │ 2   │ orders        │ Page #25    │ 1                │    │
│  │ 3   │ products      │ Page #40    │ 3                │    │
│  └─────┴──────────────┴────────────────────────────────┘    │
│                                                              │
│  Indexes for "users"                                         │
│  ┌─────┬──────────────┬────────────┬───────────────────┐    │
│  │ ID  │ Name          │ Root Page  │ Status            │    │
│  ├─────┼──────────────┼────────────┼───────────────────┤    │
│  │ 1   │ email_idx     │ Page #14   │ Ready             │    │
│  │ 2   │ age_idx       │ Page #16   │ Building          │    │
│  └─────┴──────────────┴────────────┴───────────────────┘    │
│                                                              │
│  (clicking any root page → opens B-Tree module for that tree)│
└─────────────────────────────────────────────────────────────┘
```

**Read features**:
- Scan catalog by-ID B-tree → decode entries as collection metadata.
- Scan catalog by-name B-tree → cross-reference.
- For each collection, list its indexes.
- Root page links navigate to B-Tree module.

**Write features** (require 🔓 unlocked):
- **Create collection**: [+ Collection] → input name → allocates a new data B-tree,
  inserts entries into both catalog B-trees (by-id and by-name), appends a
  CREATE_COLLECTION WAL record. Updates `next_collection_id` in FileHeader.
- **Drop collection**: Select collection → [Drop] → confirmation showing collection
  name, data root page, and index count → removes from both catalog B-trees, appends
  DROP_COLLECTION WAL record. (⚠ Does not reclaim data pages; those become orphaned
  until vacuum.)
- **Create index**: Select collection → [+ Index] → input index name + field path →
  allocates a new index B-tree, inserts into catalog, appends CREATE_INDEX WAL record.
  Shows the new index in "Building" status.
- **Drop index**: Select index → [Drop] → confirmation → removes from catalog,
  appends DROP_INDEX WAL record.
- **Mark index ready**: Select a "Building" index → [Mark Ready] → appends
  INDEX_READY WAL record, updates catalog entry status.
- **Edit catalog entry raw**: Right-click any collection/index row → "Edit raw..." →
  hex editor showing the serialized `CollectionEntry`/`IndexEntry` bytes → direct
  B-tree value update. For power users who need to fix corrupted catalog entries.

### 3.8 Collections Module (High-Level)

**Purpose**: Browse and manage collections at the **document level** — the highest
abstraction layer. This is the "friendly" interface for users who want to work with
JSON documents rather than raw B-tree entries.

```
┌──────────────────┬──────────────────────────────────────────┐
│  Collections      │  users (142 documents)                   │
│                   │                                          │
│  ▸ users    (142) │  [+ Insert]  [Query...]  [Drop All]     │
│  ▸ orders   (89)  │                                          │
│  ▸ products (56)  │  ┌────────────────────────────────────┐ │
│                   │  │ doc_id: 0a4b...  ts: 1709234567    │ │
│  [+ New Collection]│ │ {                                   │ │
│                   │  │   "name": "Alice",                  │ │
│                   │  │   "email": "alice@example.com",     │ │
│                   │  │   "age": 30                         │ │
│                   │  │ }                              [✎] [✗]│
│                   │  ├────────────────────────────────────┤ │
│                   │  │ doc_id: 1c8f...  ts: 1709234590    │ │
│                   │  │ {                                   │ │
│                   │  │   "name": "Bob",                    │ │
│                   │  │   "email": "bob@example.com",       │ │
│                   │  │   "age": 25                         │ │
│                   │  │ }                              [✎] [✗]│
│                   │  └────────────────────────────────────┘ │
│                   │                                          │
│                   │  Document Editor (bottom panel)          │
│                   │  ┌────────────────────────────────────┐ │
│                   │  │ { "name": "Alice Updated", ...     │ │
│                   │  │                    [Save] [Cancel] │ │
│                   │  └────────────────────────────────────┘ │
└──────────────────┴──────────────────────────────────────────┘
```

**How it works**: This module composes lower-level engine operations to provide a
document-store experience. It reads the catalog to discover collections, then uses
the collection's data B-tree to scan/query documents. Documents are stored as
MVCC-versioned key-value entries where key = `doc_id || inv_ts` and value = JSON blob
(potentially in heap for large docs).

**Read features**:
- List all collections with document counts.
- Browse documents in a collection (paginated scan of the data B-tree).
- View document JSON with syntax highlighting.
- Show document metadata: doc_id (hex), timestamp, version history.
- **Query**: Filter documents using the existing Filter system (Eq, Gt, Lt, In, And, Or).
  If an index exists for the filter field, uses index scan; otherwise table scan.
- **MVCC history**: Expand a document to see all historical versions (all timestamps
  for the same doc_id in the B-tree).

**Write features** (require 🔓 unlocked):
- **Insert document**: [+ Insert] → JSON editor → generates a new random doc_id,
  allocates current timestamp, serializes JSON → inserts into data B-tree as
  `key=doc_id||inv_ts, value=json_bytes`. Also inserts into secondary index B-trees.
  Appends TX_COMMIT WAL record.
- **Edit document**: Click [✎] → JSON editor pre-filled with current value →
  [Save] inserts a new version (new timestamp) into the data B-tree. Old version
  remains (MVCC). Also updates secondary indexes.
- **Delete document**: Click [✗] → confirmation → inserts a tombstone entry
  (empty value) at the current timestamp. Document no longer visible in scans.
- **Drop all documents**: [Drop All] → confirmation with document count → bulk
  delete. (⚠ Inserts tombstones for every document.)
- **Import JSON**: [Import...] → paste or load a JSON array → batch insert all
  documents.
- **Export JSON**: [Export...] → save all documents in a collection as a JSON array
  file.

### 3.9 Console Module

**Purpose**: Direct access to every engine API call. A power-user REPL for
interacting with the storage engine programmatically.

```
┌─────────────────────────────────────────────────────────────┐
│  Engine Console                                              │
│                                                              │
│  API: [StorageEngine ▾]  Method: [create_btree ▾]           │
│                                                              │
│  Parameters:                                                 │
│  (no parameters for create_btree)                            │
│                                                              │
│  [Execute]                                                   │
│                                                              │
│  ─────────────── Output ───────────────────────────────────  │
│                                                              │
│  > storage.create_btree()                                    │
│  ✓ BTreeHandle { root_page: 148 }                            │
│                                                              │
│  > storage.append_wal(0x01, [0x00, 0x01, 0x02])             │
│  ✓ LSN: 12,416                                               │
│                                                              │
│  > btree(148).insert(key: 0x0001, value: 0x48656C6C6F)      │
│  ✓ OK                                                        │
│                                                              │
│  > storage.checkpoint()                                      │
│  ✓ Checkpoint at LSN 12,416, flushed 3 dirty pages           │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

**Available API targets** (all engine layer operations exposed as callable commands):

| Target | Methods |
|--------|---------|
| `storage` | `create_btree`, `open_btree(root)`, `heap_store(data)`, `heap_free(ref)`, `append_wal(type, payload)`, `checkpoint`, `vacuum(entries)`, `update_file_header(field, value)`, `close` |
| `btree(root)` | `insert(key, value)`, `delete(key)`, `get(key)`, `scan(start?, end?, limit?)` |
| `page(id)` | `read`, `init(type)`, `set_header(field, value)`, `insert_slot(data)`, `update_slot(slot, data)`, `delete_slot(slot)`, `compact`, `stamp_checksum` |
| `heap` | `store(data)`, `free(page, slot)`, `load(page, slot)` |
| `freelist` | `allocate`, `deallocate(page_id)`, `walk` |
| `wal` | `append(type, payload)`, `append_raw(frame)`, `list_segments`, `read_frames(from_lsn, limit)` |
| `catalog` | `list_collections`, `create_collection(name)`, `drop_collection(id)`, `create_index(col_id, name, field)`, `drop_index(id)` |

**Features**:
- Dropdown-based API browser — select target + method, fill parameters.
- Output log with scrollback history.
- Results displayed as formatted structs (page IDs as clickable links).
- **Command history**: Up/down arrows to recall previous commands.
- **Scriptable**: Paste multiple commands separated by newlines → execute sequentially.
- All write commands respect the 🔓 lock and show confirmation for destructive ops.

---

## 4. Engine Layer (Read-Write Access)

### 4.1 Database Handle

```rust
/// Opens a database directory in full read-write mode.
/// Wraps StorageEngine and exposes all subsystems.
pub struct DbHandle {
    engine: Arc<StorageEngine>,
    page_size: usize,
}

impl DbHandle {
    /// Open a database directory with full read-write access.
    /// Runs WAL recovery on open. Starts WAL writer background task.
    pub async fn open(path: &Path) -> io::Result<Self>;

    /// Close the database. Runs a final checkpoint, stops WAL writer.
    pub async fn close(&self) -> io::Result<()>;

    /// Get a reference to the underlying StorageEngine for direct access.
    pub fn engine(&self) -> &StorageEngine;
}
```

Uses `StorageEngine::open` directly — full recovery, WAL writer, checkpoint support.
The UI-level RW lock toggle is enforced in the ViewModel layer, not here.

### 4.2 Page Access (Read + Write)

```rust
pub struct PageInfo {
    pub page_id: u32,
    pub page_type: PageType,
    pub header: PageHeader,
    pub num_slots: u16,
    pub free_space: u16,
    pub checksum_valid: bool,
    pub raw_bytes: Vec<u8>,
    pub slots: Vec<SlotInfo>,
}

pub struct SlotInfo {
    pub index: u16,
    pub offset: u16,
    pub length: u16,
    pub data: Vec<u8>,
}

impl DbHandle {
    // --- Read ---
    pub fn read_page(&self, page_id: u32) -> io::Result<PageInfo>;
    pub fn page_count(&self) -> u64;
    pub fn scan_page_types(&self) -> io::Result<Vec<(u32, PageType)>>;

    // --- Write ---
    /// Write raw bytes to a page buffer (full page overwrite).
    pub fn write_page_raw(&self, page_id: u32, bytes: &[u8]) -> io::Result<()>;
    /// Initialize a page as the given type (clears all content).
    pub fn init_page(&self, page_id: u32, page_type: PageType) -> io::Result<()>;
    /// Set a single page header field.
    pub fn set_page_header_field(&self, page_id: u32, field: &str, value: u64)
        -> io::Result<()>;
    /// Insert a new slot with the given cell data.
    pub fn insert_slot(&self, page_id: u32, data: &[u8]) -> io::Result<u16>;
    /// Update an existing slot's data.
    pub fn update_slot(&self, page_id: u32, slot: u16, data: &[u8]) -> io::Result<()>;
    /// Delete (tombstone) a slot.
    pub fn delete_slot(&self, page_id: u32, slot: u16) -> io::Result<()>;
    /// Compact a page (defragment cells, reclaim dead space).
    pub fn compact_page(&self, page_id: u32) -> io::Result<()>;
    /// Recompute and store the page checksum.
    pub fn stamp_checksum(&self, page_id: u32) -> io::Result<()>;
}
```

### 4.3 WAL Access (Read + Write)

```rust
pub struct WalSegmentInfo {
    pub segment_id: u32,
    pub base_lsn: u64,
    pub file_size: u64,
    pub frame_count: u64,
}

pub struct WalFrameInfo {
    pub lsn: u64,
    pub record_type: u8,
    pub record_type_name: String,
    pub payload_len: u32,
    pub crc_valid: bool,
    pub payload: Vec<u8>,
}

impl DbHandle {
    // --- Read ---
    pub fn list_wal_segments(&self) -> io::Result<Vec<WalSegmentInfo>>;
    pub fn read_wal_frames(&self, from_lsn: u64, limit: usize)
        -> io::Result<Vec<WalFrameInfo>>;

    // --- Write ---
    /// Append a typed WAL record. Returns the assigned LSN.
    pub async fn append_wal(&self, record_type: u8, payload: &[u8])
        -> io::Result<u64>;
    /// Append a pre-encoded raw WAL frame.
    pub async fn append_raw_wal_frame(&self, raw: &[u8]) -> io::Result<u64>;
}
```

### 4.4 B-Tree Access (Read + Write)

```rust
pub struct BTreeNodeInfo {
    pub page_id: u32,
    pub is_leaf: bool,
    pub num_keys: u16,
    pub entries: Vec<BTreeEntry>,
    pub children: Vec<u32>,       // page IDs (internal nodes)
    pub right_sibling: u32,       // leaf nodes
}

pub struct BTreeEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,           // leaf: value bytes, internal: child page (u32)
}

impl DbHandle {
    // --- Read ---
    pub fn read_btree_node(&self, page_id: u32) -> io::Result<BTreeNodeInfo>;
    pub fn walk_btree(&self, root_page: u32, max_depth: u32)
        -> io::Result<BTreeStructure>;

    // --- Write ---
    /// Create a new empty B-tree. Returns the root page ID.
    pub fn create_btree(&self) -> io::Result<u32>;
    /// Open an existing B-tree by root page and insert a key-value pair.
    pub fn btree_insert(&self, root_page: u32, key: &[u8], value: &[u8])
        -> io::Result<()>;
    /// Delete a key from a B-tree. Returns true if the key existed.
    pub fn btree_delete(&self, root_page: u32, key: &[u8]) -> io::Result<bool>;
}
```

### 4.5 Heap Access (Read + Write)

```rust
pub struct HeapSlotInfo {
    pub page_id: u32,
    pub slot_id: u16,
    pub flags: u8,
    pub total_length: u32,
    pub first_chunk: Vec<u8>,
    pub overflow_chain: Vec<u32>,  // page IDs in chain order
    pub has_overflow: bool,
}

impl DbHandle {
    // --- Read ---
    pub fn read_heap_slot(&self, page_id: u32, slot_id: u16)
        -> io::Result<HeapSlotInfo>;
    pub fn load_heap_blob(&self, page_id: u32, slot_id: u16)
        -> io::Result<Vec<u8>>;

    // --- Write ---
    /// Store a blob in the heap. Returns HeapRef (page_id, slot_id).
    pub fn heap_store(&self, data: &[u8]) -> io::Result<(u32, u16)>;
    /// Free a heap blob and reclaim its overflow pages.
    pub fn heap_free(&self, page_id: u32, slot_id: u16) -> io::Result<()>;
}
```

### 4.6 Free List Access (Read + Write)

```rust
impl DbHandle {
    // --- Read ---
    pub fn walk_free_list(&self) -> io::Result<Vec<u32>>;

    // --- Write ---
    /// Allocate a page from the free list (or extend the file).
    pub fn free_list_allocate(&self) -> io::Result<u32>;
    /// Return a page to the free list.
    pub fn free_list_deallocate(&self, page_id: u32) -> io::Result<()>;
}
```

### 4.7 Catalog Access (Read + Write)

```rust
pub struct CollectionInfo {
    pub id: u64,
    pub name: String,
    pub data_root_page: u32,
    pub indexes: Vec<IndexInfo>,
}

pub struct IndexInfo {
    pub id: u64,
    pub name: String,
    pub root_page: u32,
    pub status: String,  // "Ready", "Building"
}

impl DbHandle {
    // --- Read ---
    pub fn list_collections(&self) -> io::Result<Vec<CollectionInfo>>;

    // --- Write ---
    /// Create a new collection. Allocates data B-tree, inserts into catalog,
    /// appends CREATE_COLLECTION WAL record. Returns collection ID.
    pub fn create_collection(&self, name: &str) -> io::Result<u64>;
    /// Drop a collection by ID. Removes from catalog, appends DROP_COLLECTION
    /// WAL record. Does not reclaim data pages.
    pub fn drop_collection(&self, id: u64) -> io::Result<()>;
    /// Create a new index on a collection. Allocates index B-tree, inserts into
    /// catalog, appends CREATE_INDEX WAL record. Returns index ID.
    pub fn create_index(&self, collection_id: u64, name: &str, field_path: &str)
        -> io::Result<u64>;
    /// Drop an index by ID.
    pub fn drop_index(&self, id: u64) -> io::Result<()>;
    /// Mark an index as ready (transitions from Building → Ready).
    pub fn mark_index_ready(&self, id: u64) -> io::Result<()>;
}
```

### 4.8 FileHeader Access + Maintenance

```rust
impl DbHandle {
    /// Read the current FileHeader.
    pub fn read_file_header(&self) -> io::Result<FileHeader>;
    /// Update a single FileHeader field by name.
    pub fn update_file_header_field(&self, field: &str, value: u64)
        -> io::Result<()>;
    /// Trigger a checkpoint (flush dirty pages to disk).
    pub async fn checkpoint(&self) -> io::Result<u64>;
    /// Get the current dirty page count from the buffer pool.
    pub fn dirty_page_count(&self) -> usize;
    /// Run vacuum on the given entries.
    pub fn vacuum(&self, entries: &[VacuumEntry]) -> io::Result<usize>;
}
```

---

## 5. Shared Components

### 5.1 Hex Editor

A reusable hex dump component with **inline editing**:
- Displays bytes in standard `OFFSET: HH HH HH ... | ASCII` format
- Accepts highlight regions: `Vec<(range, color)>`
- Supports click-to-select a byte range
- Auto-scrolls to highlighted region
- Shows offset ruler
- **Edit mode**: Click a byte → type new hex value → byte turns yellow (modified).
  Multiple bytes can be edited before applying. [Apply] / [Revert] buttons.
- **Read-only mode**: When RW lock is engaged, editing is disabled (cursor still
  selects for inspection, but no modifications allowed).
- Reports a `Vec<(offset, old_byte, new_byte)>` changeset on apply.

### 5.2 Page Space Bar

Visual representation of page layout as a horizontal bar:

```
[HDR 32B][SLOT DIR 92B][  FREE 6723B  ][CELL DATA 1345B]
 blue      green          gray            orange
```

Proportional widths. Clickable regions. Tooltip shows byte range.

### 5.3 Key-Value Table

Generic table for displaying **and editing** decoded data:
- Two columns: label + value
- Values can be: text, hex, clickable page link, badge (for types/status)
- **Editable rows**: Rows marked as editable show a [✎] icon. Clicking opens
  an inline input (text, number, or hex depending on field type).
- Used by page header display, file header display, etc.

### 5.4 Byte Bar

Horizontal bar chart for showing space distribution (used in overview):
```
████████░░░░░ BTreeLeaf 48/147 (33%)
```

### 5.5 JSON Editor

A JSON viewer/editor component for document values:
- Syntax-highlighted JSON display with collapsible nodes.
- **Edit mode**: Switch to a text area with JSON validation. Invalid JSON shows
  inline errors. [Save] serializes and writes back.
- Supports both pretty-printed (for documents) and compact (for small values) modes.
- Used by Collections module (document editing) and Heap module (blob editing).

### 5.6 Confirmation Dialog

Modal dialog for destructive operations:
- Title: operation name (e.g., "Delete Slot", "Drop Collection").
- Body: summary of what will happen, including affected page IDs, entry counts, etc.
- Two buttons: [Cancel] (default focus) and [Confirm] (red/destructive style).
- For especially dangerous operations (raw page overwrite, WAL truncate), requires
  typing a confirmation phrase (e.g., "DROP users").

### 5.7 RW Lock Badge

Toolbar widget showing current access mode:
- **🔒 READ-ONLY** (default): Gray badge. All write UI disabled/hidden.
- **🔓 READ-WRITE**: Orange badge with pulsing border. Write UI enabled.
- Clicking the badge toggles the mode. Switching to read-write shows a confirmation
  dialog ("Enable write access? Modifications can corrupt the database.").
- Switching back to read-only is immediate (no confirmation needed).

---

## 6. Styling

Use Dioxus built-in CSS with a custom stylesheet. Dark theme by default
(database tools should feel like IDEs).

### Color Palette

| Token              | Value     | Usage                          |
|--------------------|-----------|--------------------------------|
| `--bg-primary`     | `#1e1e2e` | Main background                |
| `--bg-secondary`   | `#2a2a3e` | Panels, cards                  |
| `--bg-tertiary`    | `#363650` | Hover, selected rows           |
| `--text-primary`   | `#e0e0e0` | Main text                      |
| `--text-secondary` | `#a0a0b0` | Labels, muted text             |
| `--accent`         | `#7aa2f7` | Links, active items            |
| `--border`         | `#3a3a50` | Borders, dividers              |
| `--page-header`    | `#7aa2f7` | Page header region (blue)      |
| `--page-slots`     | `#9ece6a` | Slot directory region (green)  |
| `--page-free`      | `#565670` | Free space region (gray)       |
| `--page-cells`     | `#e0af68` | Cell data region (orange)      |
| `--page-deleted`   | `#f7768e` | Deleted/tombstone (red)        |
| `--checksum-ok`    | `#9ece6a` | Valid checksum (green)         |
| `--checksum-bad`   | `#f7768e` | Invalid checksum (red)         |
| `--write-modified` | `#e0af68` | Modified/dirty bytes (yellow)  |
| `--write-action`   | `#ff9e64` | Write action buttons (orange)  |
| `--write-danger`   | `#f7768e` | Destructive actions (red)      |
| `--rw-locked`      | `#565670` | RW lock badge (gray)           |
| `--rw-unlocked`    | `#ff9e64` | RW unlock badge (orange)       |

### Typography

- Monospace everywhere: `"JetBrains Mono", "Fira Code", monospace`
- Font size: 13px base, 11px for hex dump
- Line height: 1.5

---

## 7. State Management

### Global State

```rust
/// Global application state, shared via Dioxus context.
struct AppState {
    /// Currently open database (None if no file opened).
    db: Signal<Option<Arc<DbHandle>>>,
    /// Active module.
    active_module: Signal<Module>,
    /// Navigation stack for breadcrumb.
    breadcrumb: Signal<Vec<BreadcrumbItem>>,
    /// Read-write lock. When false, all write operations are blocked in the UI.
    /// Defaults to false (locked / read-only) on database open.
    write_enabled: Signal<bool>,
    /// Count of dirty pages in the buffer pool (polled periodically).
    dirty_page_count: Signal<usize>,
    /// Last operation result/error (displayed as a toast notification).
    last_result: Signal<Option<OperationResult>>,
}

#[derive(Clone, PartialEq)]
enum Module {
    Overview,
    Collections,
    Pages,
    BTree,
    Wal,
    Heap,
    FreeList,
    Catalog,
    Console,
}

#[derive(Clone)]
enum OperationResult {
    Success(String),  // e.g., "Inserted slot #5 on page #42"
    Error(String),    // e.g., "Page full: cannot insert 8200 bytes"
}
```

### Per-Module State

Each module maintains its own local state via signals:

```rust
// Example: Pages module state
struct PagesState {
    selected_page: Signal<Option<u32>>,
    selected_slot: Signal<Option<u16>>,
    page_type_filter: Signal<Option<PageType>>,
    page_list: Signal<Vec<(u32, PageType)>>,
    show_hex_panel: Signal<bool>,
    /// Pending hex edits (offset → new byte) before [Apply].
    pending_edits: Signal<BTreeMap<usize, u8>>,
}
```

### Write Guard Pattern

All write operations in the ViewModel layer check `write_enabled` before proceeding:

```rust
fn insert_slot(page_id: u32, data: Vec<u8>) {
    let state = use_context::<AppState>();
    if !*state.write_enabled.read() {
        return; // Write UI should already be disabled, but double-check
    }
    let db = state.db.read().clone();
    spawn(async move {
        if let Some(db) = db {
            let result = tokio::task::spawn_blocking(move || {
                db.insert_slot(page_id, &data)
            }).await.unwrap();
            match result {
                Ok(slot_id) => {
                    state.last_result.set(Some(OperationResult::Success(
                        format!("Inserted slot #{slot_id} on page #{page_id}")
                    )));
                    // Refresh page view
                }
                Err(e) => {
                    state.last_result.set(Some(OperationResult::Error(
                        e.to_string()
                    )));
                }
            }
        }
    });
}
```

### Async Pattern

All engine calls run on `spawn_blocking` to avoid blocking the UI:

```rust
fn load_page(page_id: u32) {
    let db = use_context::<AppState>().db.read().clone();
    spawn(async move {
        if let Some(db) = db {
            let info = tokio::task::spawn_blocking(move || {
                db.read_page(page_id)
            }).await.unwrap();
            // Update signal with result
        }
    });
}
```

---

## 8. Implementation Phases

### Phase 1: Skeleton + Overview (MVP)
1. Set up Dioxus 0.7 desktop project in `apps/studio/`
2. Implement layout shell (toolbar, sidebar, main area)
3. Implement `DbHandle::open` (full read-write via `StorageEngine::open`)
4. Implement RW lock toggle + confirmation dialog
5. Implement Overview module (file header display + editable fields)
6. Native file dialog for opening databases
7. Checkpoint button + dirty page count badge

### Phase 2: Pages Module (Read + Write)
1. Implement `PageReader` (decode any page)
2. Page list with type filtering
3. Page header display + inline editable fields
4. Slot directory table + insert/update/delete slot actions
5. Space map bar component
6. Hex editor component (view + inline byte editing)
7. Page init / compact / stamp checksum actions

### Phase 3: B-Tree + WAL (Read + Write)
1. B-tree node reader and tree walker
2. B-tree visualization (expandable tree)
3. B-tree insert/delete key-value pairs + create new B-tree
4. WAL segment listing and frame iteration
5. WAL frame detail view
6. WAL record append (typed + raw) + structured record builders

### Phase 4: Heap + Free List + Catalog (Read + Write)
1. Heap slot decoder and overflow chain follower
2. Heap store/free blob actions
3. Free list walker + allocate/deallocate page actions
4. Catalog reader (decode collection/index metadata)
5. Catalog writer (create/drop collection, create/drop/ready index)
6. Cross-module navigation (click page link → Pages module)

### Phase 5: Collections Module (High-Level)
1. Collection browser with document counts
2. Document list with JSON viewer
3. Document insert/edit/delete (JSON editor)
4. Query with filter builder (Eq, Gt, Lt, In, And, Or)
5. MVCC version history viewer
6. Import/export JSON

### Phase 6: Console Module
1. API target + method dropdown browser
2. Parameter input forms (hex, numbers, strings)
3. Execute + output log with scrollback
4. Command history + scriptable multi-command execution
5. Clickable page ID links in output

### Phase 7: Polish
1. Keyboard navigation (arrow keys in lists, Esc to go back)
2. Search/filter in all list views
3. Copy-to-clipboard for hex values
4. Responsive resize handling
5. Error states, loading indicators, and toast notifications
6. Undo for single-step operations (where feasible — e.g., slot delete
   can be undone by re-inserting the data)

---

## 9. Open Questions / Future Modules

These are deferred but the module system makes them easy to add later:

- **Diff Module**: Compare two database snapshots side by side
- **Timeline Module**: Visualize WAL as a timeline of operations
- **MVCC Module**: Decode versioned keys and show version chains
- **Posting List Module**: Inspect posting list encoded data
- **Replication Module**: Connect to a primary, view replication lag, inspect
  snapshot frames
- **Fuzzer Module**: Generate random mutations (inserts, deletes, page corruptions)
  for stress-testing the storage engine
