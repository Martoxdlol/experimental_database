/// CSS theme constants for the dark IDE-like UI.
pub const STYLESHEET: &str = r#"
:root {
    --bg-primary: #1e1e2e;
    --bg-secondary: #2a2a3e;
    --bg-tertiary: #363650;
    --text-primary: #e0e0e0;
    --text-secondary: #a0a0b0;
    --accent: #7aa2f7;
    --border: #3a3a50;
    --page-header: #7aa2f7;
    --page-slots: #9ece6a;
    --page-free: #565670;
    --page-cells: #e0af68;
    --page-deleted: #f7768e;
    --checksum-ok: #9ece6a;
    --checksum-bad: #f7768e;
    --write-modified: #e0af68;
    --write-action: #ff9e64;
    --write-danger: #f7768e;
    --rw-locked: #565670;
    --rw-unlocked: #ff9e64;
}

* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: "JetBrains Mono", "Fira Code", "Cascadia Code", monospace;
    font-size: 13px;
    line-height: 1.5;
    color: var(--text-primary);
    background: var(--bg-primary);
    overflow: hidden;
}

/* Layout */
.app-container {
    display: flex;
    flex-direction: column;
    height: 100vh;
    width: 100vw;
}

/* Toolbar */
.toolbar {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 8px 16px;
    background: var(--bg-secondary);
    border-bottom: 1px solid var(--border);
    min-height: 44px;
    flex-shrink: 0;
}

.toolbar-group {
    display: flex;
    align-items: center;
    gap: 8px;
}

.toolbar-separator {
    width: 1px;
    height: 24px;
    background: var(--border);
}

.toolbar-info {
    color: var(--text-secondary);
    font-size: 12px;
    flex: 1;
}

/* Buttons */
.btn {
    padding: 4px 12px;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--bg-tertiary);
    color: var(--text-primary);
    cursor: pointer;
    font-family: inherit;
    font-size: 12px;
    white-space: nowrap;
    transition: background 0.15s;
}

.btn:hover {
    background: var(--accent);
    color: #fff;
}

.btn:disabled {
    opacity: 0.4;
    cursor: not-allowed;
}

.btn:disabled:hover {
    background: var(--bg-tertiary);
    color: var(--text-primary);
}

.btn-danger {
    border-color: var(--write-danger);
    color: var(--write-danger);
}

.btn-danger:hover {
    background: var(--write-danger);
    color: #fff;
}

.btn-action {
    border-color: var(--write-action);
    color: var(--write-action);
}

.btn-action:hover {
    background: var(--write-action);
    color: #fff;
}

/* RW Lock Badge */
.rw-badge {
    padding: 4px 10px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: bold;
    cursor: pointer;
    border: 2px solid;
    transition: all 0.15s;
    user-select: none;
}

.rw-badge.locked {
    background: transparent;
    border-color: var(--rw-locked);
    color: var(--rw-locked);
}

.rw-badge.unlocked {
    background: transparent;
    border-color: var(--rw-unlocked);
    color: var(--rw-unlocked);
    animation: pulse-border 2s infinite;
}

@keyframes pulse-border {
    0%, 100% { border-color: var(--rw-unlocked); }
    50% { border-color: transparent; }
}

/* Main Layout */
.main-layout {
    display: flex;
    flex: 1;
    overflow: hidden;
}

/* Sidebar */
.sidebar {
    width: 56px;
    background: var(--bg-secondary);
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    padding: 8px 0;
    flex-shrink: 0;
}

.sidebar-item {
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 8px 4px;
    cursor: pointer;
    color: var(--text-secondary);
    font-size: 10px;
    text-align: center;
    transition: all 0.15s;
    border-left: 3px solid transparent;
    user-select: none;
}

.sidebar-item:hover {
    color: var(--text-primary);
    background: var(--bg-tertiary);
}

.sidebar-item.active {
    color: var(--accent);
    border-left-color: var(--accent);
    background: rgba(122, 162, 247, 0.1);
}

.sidebar-item.disabled {
    opacity: 0.3;
    cursor: not-allowed;
}

.sidebar-icon {
    font-size: 18px;
    margin-bottom: 2px;
}

/* Content */
.content {
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: hidden;
}

/* Breadcrumb */
.breadcrumb {
    padding: 6px 16px;
    background: var(--bg-primary);
    border-bottom: 1px solid var(--border);
    color: var(--text-secondary);
    font-size: 12px;
    flex-shrink: 0;
}

.breadcrumb-separator {
    margin: 0 6px;
    color: var(--border);
}

.breadcrumb-link {
    color: var(--accent);
    cursor: pointer;
}

.breadcrumb-link:hover {
    text-decoration: underline;
}

/* Main content area */
.main-content {
    flex: 1;
    overflow: auto;
    padding: 16px;
}

/* Cards / Panels */
.card {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: 6px;
    margin-bottom: 16px;
}

.card-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
    font-weight: bold;
    font-size: 13px;
}

.card-body {
    padding: 12px 16px;
}

/* Key-Value Table */
.kv-table {
    width: 100%;
    border-collapse: collapse;
}

.kv-table tr {
    border-bottom: 1px solid rgba(58, 58, 80, 0.5);
}

.kv-table tr:last-child {
    border-bottom: none;
}

.kv-table td {
    padding: 4px 0;
}

.kv-table td:first-child {
    color: var(--text-secondary);
    width: 180px;
    padding-right: 16px;
}

.kv-table td:last-child {
    display: flex;
    align-items: center;
    gap: 8px;
}

.kv-value {
    flex: 1;
}

/* Editable field */
.edit-btn {
    background: none;
    border: none;
    color: var(--text-secondary);
    cursor: pointer;
    padding: 2px 4px;
    font-size: 12px;
    opacity: 0.5;
    transition: opacity 0.15s;
}

.edit-btn:hover {
    opacity: 1;
    color: var(--accent);
}

.edit-input {
    background: var(--bg-primary);
    border: 1px solid var(--accent);
    border-radius: 3px;
    color: var(--text-primary);
    font-family: inherit;
    font-size: 13px;
    padding: 2px 6px;
    width: 160px;
}

/* Space Bar */
.space-bar {
    display: flex;
    height: 24px;
    border-radius: 4px;
    overflow: hidden;
    border: 1px solid var(--border);
}

.space-bar-segment {
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 10px;
    color: #000;
    overflow: hidden;
    white-space: nowrap;
}

/* Master-Detail Layout */
.master-detail {
    display: flex;
    height: 100%;
    overflow: hidden;
}

.master-panel {
    width: 220px;
    border-right: 1px solid var(--border);
    overflow-y: auto;
    flex-shrink: 0;
}

.detail-panel {
    flex: 1;
    overflow-y: auto;
    padding: 16px;
}

/* List items */
.list-item {
    padding: 6px 12px;
    cursor: pointer;
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 12px;
    transition: background 0.1s;
    border-bottom: 1px solid rgba(58, 58, 80, 0.3);
}

.list-item:hover {
    background: var(--bg-tertiary);
}

.list-item.selected {
    background: rgba(122, 162, 247, 0.15);
    color: var(--accent);
}

/* Page type badges */
.page-type-badge {
    padding: 1px 6px;
    border-radius: 3px;
    font-size: 10px;
    font-weight: bold;
}

.page-type-badge.btree-leaf { background: var(--page-cells); color: #000; }
.page-type-badge.btree-internal { background: var(--page-header); color: #000; }
.page-type-badge.heap { background: #bb9af7; color: #000; }
.page-type-badge.overflow { background: #f7768e; color: #000; }
.page-type-badge.free { background: var(--page-free); color: #fff; }
.page-type-badge.file-header { background: #73daca; color: #000; }

/* Status badges */
.status-badge {
    padding: 1px 6px;
    border-radius: 3px;
    font-size: 10px;
}

.status-badge.ok { background: var(--checksum-ok); color: #000; }
.status-badge.bad { background: var(--checksum-bad); color: #000; }

/* Hex Dump */
.hex-dump {
    font-size: 11px;
    line-height: 1.6;
    white-space: pre;
    overflow: auto;
    padding: 8px;
    background: var(--bg-primary);
    border-radius: 4px;
    border: 1px solid var(--border);
    max-height: 400px;
}

.hex-offset {
    color: var(--text-secondary);
}

.hex-byte {
    cursor: pointer;
    padding: 0 1px;
}

.hex-byte:hover {
    background: var(--bg-tertiary);
}

.hex-byte.selected {
    background: var(--accent);
    color: #fff;
}

.hex-byte.modified {
    background: var(--write-modified);
    color: #000;
}

.hex-ascii {
    color: var(--text-secondary);
}

/* Slot table */
.slot-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
}

.slot-table th {
    text-align: left;
    padding: 4px 8px;
    color: var(--text-secondary);
    border-bottom: 1px solid var(--border);
    font-weight: normal;
}

.slot-table td {
    padding: 4px 8px;
    border-bottom: 1px solid rgba(58, 58, 80, 0.3);
}

.slot-table tr:hover {
    background: var(--bg-tertiary);
}

.slot-table tr.selected {
    background: rgba(122, 162, 247, 0.15);
}

.slot-deleted {
    color: var(--page-deleted);
    font-style: italic;
}

/* Tree view */
.tree-node {
    padding-left: 20px;
}

.tree-node-header {
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 3px 0;
    cursor: pointer;
}

.tree-node-header:hover {
    color: var(--accent);
}

.tree-toggle {
    width: 16px;
    text-align: center;
    color: var(--text-secondary);
}

.tree-label {
    font-size: 12px;
}

/* Toast notifications */
.toast {
    position: fixed;
    bottom: 16px;
    right: 16px;
    padding: 10px 16px;
    border-radius: 6px;
    font-size: 12px;
    max-width: 400px;
    z-index: 1000;
    animation: toast-in 0.3s ease-out;
}

.toast.success {
    background: var(--checksum-ok);
    color: #000;
}

.toast.error {
    background: var(--write-danger);
    color: #fff;
}

@keyframes toast-in {
    from { transform: translateY(20px); opacity: 0; }
    to { transform: translateY(0); opacity: 1; }
}

/* Confirm dialog */
.dialog-overlay {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.6);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 999;
}

.dialog {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 24px;
    min-width: 360px;
    max-width: 500px;
}

.dialog-title {
    font-size: 15px;
    font-weight: bold;
    margin-bottom: 12px;
}

.dialog-body {
    color: var(--text-secondary);
    margin-bottom: 20px;
    font-size: 12px;
    line-height: 1.6;
}

.dialog-actions {
    display: flex;
    justify-content: flex-end;
    gap: 8px;
}

/* Filter input */
.filter-input {
    width: 100%;
    padding: 6px 10px;
    background: var(--bg-primary);
    border: 1px solid var(--border);
    border-radius: 4px;
    color: var(--text-primary);
    font-family: inherit;
    font-size: 12px;
    margin: 8px 0;
}

.filter-input:focus {
    outline: none;
    border-color: var(--accent);
}

/* Select dropdown */
.select {
    padding: 4px 8px;
    background: var(--bg-primary);
    border: 1px solid var(--border);
    border-radius: 4px;
    color: var(--text-primary);
    font-family: inherit;
    font-size: 12px;
}

.select:focus {
    outline: none;
    border-color: var(--accent);
}

/* Scrollbar */
::-webkit-scrollbar {
    width: 8px;
    height: 8px;
}

::-webkit-scrollbar-track {
    background: var(--bg-primary);
}

::-webkit-scrollbar-thumb {
    background: var(--border);
    border-radius: 4px;
}

::-webkit-scrollbar-thumb:hover {
    background: var(--text-secondary);
}

/* Empty state */
.empty-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100%;
    color: var(--text-secondary);
    gap: 16px;
}

.empty-state-icon {
    font-size: 48px;
    opacity: 0.3;
}

/* JSON editor */
.json-editor {
    width: 100%;
    min-height: 200px;
    padding: 8px;
    background: var(--bg-primary);
    border: 1px solid var(--border);
    border-radius: 4px;
    color: var(--text-primary);
    font-family: inherit;
    font-size: 12px;
    resize: vertical;
}

.json-editor:focus {
    outline: none;
    border-color: var(--accent);
}

.json-error {
    color: var(--write-danger);
    font-size: 11px;
    margin-top: 4px;
}

/* Console output */
.console-output {
    background: var(--bg-primary);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 8px;
    font-size: 12px;
    overflow-y: auto;
    max-height: 500px;
}

.console-line {
    padding: 2px 0;
    border-bottom: 1px solid rgba(58, 58, 80, 0.2);
}

.console-cmd {
    color: var(--text-secondary);
}

.console-ok {
    color: var(--checksum-ok);
}

.console-err {
    color: var(--write-danger);
}

/* Clickable page link */
.page-link {
    color: var(--accent);
    cursor: pointer;
    text-decoration: none;
}

.page-link:hover {
    text-decoration: underline;
}

/* Layer Tab Bar */
.layer-tabs {
    display: flex;
    gap: 0;
    background: var(--bg-secondary);
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
    padding: 0 8px;
}

.layer-tab {
    padding: 6px 16px;
    font-size: 12px;
    color: var(--text-secondary);
    cursor: pointer;
    border-bottom: 2px solid transparent;
    transition: all 0.15s;
    user-select: none;
}

.layer-tab:hover {
    color: var(--text-primary);
    background: rgba(122, 162, 247, 0.05);
}

.layer-tab.active {
    color: var(--accent);
    border-bottom-color: var(--accent);
}

.layer-tab.disabled {
    opacity: 0.3;
    cursor: not-allowed;
}

/* Docstore badges */
.tombstone-badge {
    padding: 1px 6px;
    border-radius: 3px;
    font-size: 10px;
    font-weight: bold;
    background: var(--page-deleted);
    color: #fff;
    margin-left: 6px;
}

.external-badge {
    padding: 1px 6px;
    border-radius: 3px;
    font-size: 10px;
    font-weight: bold;
    background: #bb9af7;
    color: #000;
    margin-left: 6px;
}

.scalar-tag {
    padding: 1px 4px;
    border-radius: 2px;
    font-size: 10px;
    background: var(--bg-tertiary);
    color: var(--text-secondary);
    margin-right: 4px;
}

/* ═══════════════════════════════════════════════════════════════════
   New Studio Navigation + High-Level Module Styles
   ═══════════════════════════════════════════════════════════════════ */

/* Nav Sidebar */
.nav-sidebar {
    width: 200px;
    background: var(--bg-secondary);
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    padding: 8px 0;
    flex-shrink: 0;
    overflow-y: auto;
}

.nav-section-header {
    padding: 12px 16px 4px;
    font-size: 10px;
    font-weight: bold;
    color: var(--text-secondary);
    letter-spacing: 0.5px;
    user-select: none;
}

.nav-section-header.clickable {
    cursor: pointer;
}

.nav-section-header.clickable:hover {
    color: var(--accent);
}

.nav-group-header {
    padding: 8px 16px 2px 24px;
    font-size: 10px;
    font-weight: bold;
    color: var(--text-secondary);
    letter-spacing: 0.3px;
}

.nav-item {
    padding: 6px 16px;
    cursor: pointer;
    font-size: 12px;
    color: var(--text-primary);
    transition: background 0.1s;
    user-select: none;
}

.nav-item.nested {
    padding-left: 36px;
    font-size: 11px;
}

.nav-item:hover {
    background: var(--bg-tertiary);
}

.nav-item.active {
    background: var(--bg-tertiary);
    color: var(--accent);
    border-left: 3px solid var(--accent);
    padding-left: 13px;
}

.nav-item.nested.active {
    padding-left: 33px;
}

.nav-item.disabled {
    opacity: 0.4;
    cursor: not-allowed;
}

.nav-item.disabled:hover {
    background: transparent;
}

/* Tab Bar (Documents / Indexes toggle) */
.tab-bar {
    display: flex;
    gap: 0;
    border-bottom: 1px solid var(--border);
    margin-bottom: 12px;
}

.tab {
    padding: 8px 20px;
    cursor: pointer;
    font-size: 13px;
    color: var(--text-secondary);
    border-bottom: 2px solid transparent;
    transition: all 0.15s;
    user-select: none;
}

.tab:hover {
    color: var(--text-primary);
}

.tab.active {
    color: var(--accent);
    border-bottom-color: var(--accent);
}

/* Collection Header */
.collection-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 12px;
}

.collection-title {
    margin: 0;
    font-size: 16px;
}

/* Document Cards */
.doc-card {
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 8px 12px;
    margin-bottom: 4px;
    cursor: pointer;
    transition: border-color 0.1s;
}

.doc-card:hover {
    border-color: var(--accent);
}

.doc-card.selected {
    border-color: var(--accent);
    background: var(--bg-secondary);
}

.doc-card-header {
    display: flex;
    align-items: center;
    gap: 12px;
}

.doc-id {
    font-size: 11px;
    color: var(--accent);
    flex-shrink: 0;
}

.doc-preview {
    font-size: 11px;
    color: var(--text-secondary);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

.doc-json {
    margin-top: 8px;
    padding: 8px;
    background: var(--bg-primary);
    border-radius: 4px;
    font-size: 11px;
    overflow-x: auto;
    max-height: 400px;
    overflow-y: auto;
}

.doc-actions {
    display: flex;
    gap: 8px;
    margin-top: 8px;
    padding-top: 8px;
    border-top: 1px solid var(--border);
}

/* Index Badges */
.index-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 3px;
    font-size: 11px;
    font-weight: bold;
}

.index-badge.ready {
    background: #2a3d2a;
    color: var(--checksum-ok);
}

.index-badge.building {
    background: #3d3a2a;
    color: var(--write-modified);
}

.index-badge.dropping {
    background: #3d2a2a;
    color: var(--write-danger);
}

/* JSON Editor */
.json-editor {
    width: 100%;
    padding: 8px;
    font-family: inherit;
    font-size: 12px;
    background: var(--bg-primary);
    color: var(--text-primary);
    border: 1px solid var(--border);
    border-radius: 4px;
    resize: vertical;
    tab-size: 2;
}

.json-valid {
    color: var(--checksum-ok);
    font-size: 11px;
}

.json-invalid {
    color: var(--write-danger);
    font-size: 11px;
}

/* Data Table (extends existing styles) */
.data-table {
    width: 100%;
    border-collapse: collapse;
}

.data-table th {
    text-align: left;
    padding: 6px 12px;
    border-bottom: 1px solid var(--border);
    color: var(--text-secondary);
    font-size: 11px;
    font-weight: bold;
}

.data-table td {
    padding: 6px 12px;
    border-bottom: 1px solid var(--border);
    font-size: 12px;
}

.data-table tr:hover {
    background: var(--bg-tertiary);
}

.clickable-row {
    cursor: pointer;
}

.table-scroll {
    overflow-x: auto;
}

/* Form Layout */
.form-row {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
}

.form-grid {
    display: flex;
    flex-direction: column;
    gap: 4px;
}

.form-row label {
    font-size: 12px;
    color: var(--text-secondary);
    white-space: nowrap;
}

/* Buttons */
.btn-small {
    padding: 2px 8px;
    font-size: 11px;
}

/* Error Message */
.error-message {
    padding: 8px 12px;
    background: #3d2a2a;
    border: 1px solid var(--write-danger);
    border-radius: 4px;
    color: var(--write-danger);
    font-size: 12px;
    margin-bottom: 8px;
}

/* ═══════════════════════════════════════════════════════════════════
   Collection Detail — full-bleed layout
   ═══════════════════════════════════════════════════════════════════ */

.collection-detail {
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: hidden;
    min-height: 0;
}

.collection-bar {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 0 12px;
    height: 36px;
    background: var(--bg-secondary);
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
}

.collection-bar-name {
    font-weight: 600;
    font-size: 13px;
    margin-right: 12px;
}

.collection-bar-tabs {
    display: flex;
    gap: 0;
    height: 100%;
}

.bar-tab {
    display: flex;
    align-items: center;
    padding: 0 14px;
    font-size: 12px;
    color: var(--text-secondary);
    cursor: pointer;
    border-bottom: 2px solid transparent;
    transition: all 0.1s;
    user-select: none;
}

.bar-tab:hover { color: var(--text-primary); }
.bar-tab.active {
    color: var(--accent);
    border-bottom-color: var(--accent);
}

/* ═══════════════════════════════════════════════════════════════════
   Studio Data Grid — full-screen table (Prisma/Beekeeper style)
   ═══════════════════════════════════════════════════════════════════ */

/* Outer panel — fills all remaining space */
.grid-panel {
    display: flex;
    flex-direction: column;
    flex: 1;
    min-height: 0;
    overflow: hidden;
}

/* Compact toolbar */
.grid-toolbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 4px 12px;
    background: var(--bg-secondary);
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
    height: 36px;
}

.grid-toolbar-left,
.grid-toolbar-right {
    display: flex;
    align-items: center;
    gap: 6px;
}

.grid-toolbar-sep {
    width: 1px;
    height: 18px;
    background: var(--border);
}

.grid-status {
    font-size: 11px;
    color: var(--text-secondary);
    padding: 0 4px;
}

/* Toolbar buttons */
.grid-btn {
    padding: 3px 10px;
    border: 1px solid var(--border);
    border-radius: 3px;
    background: transparent;
    color: var(--text-primary);
    cursor: pointer;
    font-family: inherit;
    font-size: 11px;
    transition: all 0.1s;
}

.grid-btn:hover { background: var(--bg-tertiary); }
.grid-btn:disabled { opacity: 0.3; cursor: not-allowed; }
.grid-btn:disabled:hover { background: transparent; }
.grid-btn.accent { border-color: var(--accent); color: var(--accent); }
.grid-btn.accent:hover { background: var(--accent); color: #fff; }
.grid-btn.active { background: var(--bg-tertiary); }

.grid-select {
    padding: 3px 6px;
    border: 1px solid var(--border);
    border-radius: 3px;
    background: var(--bg-primary);
    color: var(--text-primary);
    font-family: inherit;
    font-size: 11px;
}

/* Insert bar */
.grid-insert-bar {
    padding: 8px 12px;
    background: var(--bg-primary);
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
}

.grid-insert-editor {
    width: 100%;
    padding: 6px 8px;
    font-family: inherit;
    font-size: 12px;
    background: var(--bg-secondary);
    color: var(--text-primary);
    border: 1px solid var(--border);
    border-radius: 3px;
    resize: none;
    tab-size: 2;
    margin-bottom: 6px;
}

.grid-insert-actions {
    display: flex;
    align-items: center;
    gap: 8px;
}

/* Scrollable grid area — fills all remaining space */
.grid-scroll {
    flex: 1;
    overflow: auto;
    min-height: 0;
}

.grid-empty {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100%;
    color: var(--text-secondary);
    font-size: 14px;
}

/* The grid table */
.studio-grid {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    table-layout: auto;
}

.studio-grid thead {
    position: sticky;
    top: 0;
    z-index: 2;
}

.studio-grid thead th {
    background: var(--bg-secondary);
    padding: 6px 12px;
    font-size: 11px;
    font-weight: 600;
    color: var(--text-secondary);
    text-align: left;
    border-bottom: 2px solid var(--border);
    white-space: nowrap;
    user-select: none;
}

/* Row number gutter */
.grid-row-num {
    width: 48px;
    min-width: 48px;
    max-width: 48px;
    text-align: center !important;
    color: var(--text-secondary) !important;
    font-size: 11px !important;
    cursor: pointer;
    user-select: none;
    background: var(--bg-secondary);
    border-right: 1px solid var(--border);
    padding: 4px 0 !important;
}

/* Actions column */
.grid-actions-col { width: 36px; min-width: 36px; }
.grid-actions-cell {
    width: 36px;
    text-align: center;
    padding: 2px !important;
    opacity: 0;
    transition: opacity 0.1s;
}
.grid-row:hover .grid-actions-cell { opacity: 1; }

.btn-icon {
    width: 22px; height: 22px;
    border: none; background: transparent;
    color: var(--text-secondary);
    cursor: pointer; border-radius: 3px;
    font-size: 12px;
    display: inline-flex; align-items: center; justify-content: center;
    transition: all 0.1s;
}
.btn-icon:hover { background: var(--bg-tertiary); color: var(--text-primary); }
.btn-icon-danger:hover { background: #3d2a2a; color: var(--write-danger); }

/* Data rows */
.grid-row { transition: background 0.05s; }
.grid-row:hover { background: rgba(122, 162, 247, 0.04); }
.grid-row.expanded { background: rgba(122, 162, 247, 0.08); }
.grid-row:hover .grid-row-num {
    color: var(--accent) !important;
    background: rgba(122, 162, 247, 0.06);
}

/* Data cells */
.grid-cell {
    padding: 4px 12px;
    font-size: 12px;
    border-bottom: 1px solid var(--border);
    max-width: 300px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    cursor: default;
}
.grid-cell:hover { background: rgba(122, 162, 247, 0.06); }
.null-cell .cell-text { color: var(--text-secondary); font-style: italic; font-size: 11px; }

/* Editing */
.grid-cell.editing { padding: 0; background: var(--bg-primary); border: 2px solid var(--accent); }
.grid-cell-input {
    width: 100%; padding: 3px 10px;
    border: none; background: transparent;
    color: var(--text-primary); font-family: inherit; font-size: 12px; outline: none;
}

/* Expanded detail */
.grid-detail-row td { padding: 0 !important; border-bottom: 2px solid var(--accent); }
.row-detail {
    padding: 8px 12px 8px 60px;
    background: var(--bg-secondary);
}
.row-detail .doc-json { margin: 0; max-height: 300px; font-size: 11px; }

/* Column header */
.grid-col-header { position: relative; }

/* Event Log */
.event-log {
    max-height: 300px;
    overflow-y: auto;
}

.event-entry {
    padding: 4px 8px;
    font-size: 11px;
    border-bottom: 1px solid var(--border);
    color: var(--text-secondary);
}
"#;
