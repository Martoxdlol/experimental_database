import { useCallback, useEffect, useRef, useState } from "react";
import { StudioClient } from "./db";
import type { DocRow, Filter, IndexInfo } from "./db";
import { DataGrid } from "./components/DataGrid";
import type { GridColumn } from "./components/DataGrid";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  AlertCircle,
  CheckCircle2,
  ChevronRight,
  Circle,
  Database,
  FilePlus,
  Loader2,
  Plus,
  RefreshCw,
  RotateCcw,
  Search,
  Trash2,
  Wifi,
  WifiOff,
  X,
  Zap,
  ZapOff,
} from "lucide-react";
import "./index.css";

// ── Column inference ───────────────────────────────────────────────────────────

function inferColumns(rows: DocRow[]): GridColumn[] {
  const keys = new Set<string>();
  for (const row of rows) {
    for (const key of Object.keys(row.doc)) {
      keys.add(key);
    }
  }
  const cols: GridColumn[] = [{ key: "_id", label: "_id", width: 260 }];
  for (const key of keys) {
    cols.push({ key, label: key, width: 160 });
  }
  return cols;
}

function flattenRows(rows: DocRow[]): Record<string, unknown>[] {
  return rows.map((r) => ({ _id: r._id, ...r.doc }));
}

function parseFilter(text: string): Filter | null | string {
  const trimmed = text.trim();
  if (!trimmed) return null;
  try {
    return JSON.parse(trimmed) as Filter;
  } catch {
    return "Invalid JSON";
  }
}

// ── Status badge ──────────────────────────────────────────────────────────────

function StatusBadge({ state }: { state: string }) {
  const isReady    = state.startsWith("Ready");
  const isBuilding = state.startsWith("Building");
  return (
    <span
      className={[
        "inline-flex items-center gap-1 text-xs px-1.5 py-0.5 rounded font-mono",
        isReady    ? "bg-green-500/15 text-green-600 dark:text-green-400" :
        isBuilding ? "bg-yellow-500/15 text-yellow-600 dark:text-yellow-400" :
                     "bg-destructive/15 text-destructive",
      ].join(" ")}
    >
      {isReady ? <CheckCircle2 className="size-3" /> :
       isBuilding ? <Loader2 className="size-3 animate-spin" /> :
       <AlertCircle className="size-3" />}
      {isReady ? "Ready" : isBuilding ? "Building" : "Error"}
    </span>
  );
}

// ── App ───────────────────────────────────────────────────────────────────────

export function App() {
  // Connection
  const [url, setUrl]           = useState("ws://127.0.0.1:3000/ws");
  const [client, setClient]     = useState<StudioClient | null>(null);
  const [connecting, setConnecting] = useState(false);
  const [connError, setConnError]   = useState<string | null>(null);

  // Collections
  const [collections, setCollections] = useState<string[]>([]);
  const [selectedColl, setSelectedColl] = useState<string | null>(null);
  const [newCollName, setNewCollName]   = useState("");
  const [showNewColl, setShowNewColl]   = useState(false);

  // Indexes
  const [indexes, setIndexes]         = useState<IndexInfo[]>([]);
  const [showIndexes, setShowIndexes] = useState(false);
  const [showNewIdx, setShowNewIdx]   = useState(false);
  const [newIdxField, setNewIdxField] = useState("");
  const [newIdxUnique, setNewIdxUnique] = useState(false);

  // Documents
  const [rows, setRows]             = useState<DocRow[]>([]);
  const [loading, setLoading]       = useState(false);
  const [loadError, setLoadError]   = useState<string | null>(null);
  const [filterText, setFilterText] = useState("");
  const [filterError, setFilterError] = useState<string | null>(null);
  const [columns, setColumns]       = useState<GridColumn[]>([]);

  // Selected doc / editor
  const [selectedDocId, setSelectedDocId] = useState<string | null>(null);
  const [editorJson, setEditorJson]        = useState("");
  const [editorMode, setEditorMode]        = useState<"view" | "edit" | "new">("view");
  const [editorError, setEditorError]      = useState<string | null>(null);
  const [saving, setSaving]                = useState(false);

  // Subscription
  const [subscribed, setSubscribed] = useState(false);
  const cancelSubRef = useRef<(() => void) | null>(null);

  // Status
  const [status, setStatus] = useState("");

  // ── Connection ─────────────────────────────────────────────────────────────

  const connect = useCallback(async () => {
    setConnecting(true);
    setConnError(null);
    setStatus("Connecting…");
    try {
      const c = await StudioClient.connect(url, 5000);
      setClient(c);
      setStatus("Connected");
      setCollections([]);
      setSelectedColl(null);
      setRows([]);
      setIndexes([]);
      // Load collections immediately
      const cols = await c.listCollections();
      setCollections(cols);
    } catch (e) {
      setConnError(String(e));
      setStatus("Connection failed");
    } finally {
      setConnecting(false);
    }
  }, [url]);

  const disconnect = useCallback(() => {
    cancelSubRef.current?.();
    cancelSubRef.current = null;
    setSubscribed(false);
    client?.close();
    setClient(null);
    setCollections([]);
    setSelectedColl(null);
    setRows([]);
    setIndexes([]);
    setStatus("Disconnected");
  }, [client]);

  // ── Reload collections ─────────────────────────────────────────────────────

  const reloadCollections = useCallback(async () => {
    if (!client) return;
    try {
      const cols = await client.listCollections();
      setCollections(cols);
    } catch (e) {
      setStatus(`Error: ${String(e)}`);
    }
  }, [client]);

  // ── Select collection ──────────────────────────────────────────────────────

  const loadDocs = useCallback(
    async (coll: string, filter: Filter | null) => {
      if (!client) return;
      setLoading(true);
      setLoadError(null);
      setStatus(`Loading ${coll}…`);
      try {
        const docs = await client.findDocs(coll, filter);
        setRows(docs);
        setColumns(inferColumns(docs));
        setSelectedDocId(null);
        setEditorJson("");
        setEditorMode("view");
        setStatus(`${docs.length} document${docs.length === 1 ? "" : "s"}`);
      } catch (e) {
        setLoadError(String(e));
        setStatus(`Error: ${String(e)}`);
      } finally {
        setLoading(false);
      }
    },
    [client],
  );

  const selectCollection = useCallback(
    async (name: string) => {
      // Cancel existing subscription
      cancelSubRef.current?.();
      cancelSubRef.current = null;
      setSubscribed(false);

      setSelectedColl(name);
      setShowIndexes(false);
      setFilterText("");
      setFilterError(null);

      if (!client) return;

      // Load indexes
      try {
        const idxs = await client.listIndexes(name);
        setIndexes(idxs);
      } catch {
        setIndexes([]);
      }

      await loadDocs(name, null);
    },
    [client, loadDocs],
  );

  // ── Filter apply ───────────────────────────────────────────────────────────

  const applyFilter = useCallback(async () => {
    if (!selectedColl) return;
    const result = parseFilter(filterText);
    if (typeof result === "string") {
      setFilterError(result);
      return;
    }
    setFilterError(null);
    await loadDocs(selectedColl, result);
  }, [filterText, selectedColl, loadDocs]);

  // ── Subscription ───────────────────────────────────────────────────────────

  const toggleSubscription = useCallback(async () => {
    if (!client || !selectedColl) return;

    if (subscribed) {
      cancelSubRef.current?.();
      cancelSubRef.current = null;
      setSubscribed(false);
      setStatus("Unsubscribed");
      return;
    }

    const result = parseFilter(filterText);
    const filter = typeof result === "string" ? null : result;
    try {
      const { cancel } = await client.subscribe(selectedColl, filter, async () => {
        setStatus("Invalidated — refreshing…");
        await loadDocs(selectedColl, filter);
      });
      cancelSubRef.current = cancel;
      setSubscribed(true);
      setStatus("Live subscription active");
    } catch (e) {
      setStatus(`Subscribe error: ${String(e)}`);
    }
  }, [client, selectedColl, subscribed, filterText, loadDocs]);

  // ── Create collection ──────────────────────────────────────────────────────

  const createCollection = useCallback(async () => {
    if (!client || !newCollName.trim()) return;
    try {
      await client.createCollection(newCollName.trim());
      setNewCollName("");
      setShowNewColl(false);
      await reloadCollections();
      setStatus(`Created collection "${newCollName.trim()}"`);
    } catch (e) {
      setStatus(`Error: ${String(e)}`);
    }
  }, [client, newCollName, reloadCollections]);

  // ── Delete collection ──────────────────────────────────────────────────────

  const deleteCollection = useCallback(async () => {
    if (!client || !selectedColl) return;
    if (!confirm(`Delete collection "${selectedColl}" and all its documents?`)) return;
    try {
      await client.deleteCollection(selectedColl);
      setSelectedColl(null);
      setRows([]);
      setIndexes([]);
      cancelSubRef.current?.();
      cancelSubRef.current = null;
      setSubscribed(false);
      await reloadCollections();
      setStatus(`Deleted collection "${selectedColl}"`);
    } catch (e) {
      setStatus(`Error: ${String(e)}`);
    }
  }, [client, selectedColl, reloadCollections]);

  // ── Create index ───────────────────────────────────────────────────────────

  const createIndex = useCallback(async () => {
    if (!client || !selectedColl || !newIdxField.trim()) return;
    try {
      await client.createIndex(selectedColl, newIdxField.trim(), newIdxUnique);
      setNewIdxField("");
      setNewIdxUnique(false);
      setShowNewIdx(false);
      const idxs = await client.listIndexes(selectedColl);
      setIndexes(idxs);
      setStatus(`Created index on "${newIdxField.trim()}"`);
    } catch (e) {
      setStatus(`Error: ${String(e)}`);
    }
  }, [client, selectedColl, newIdxField, newIdxUnique]);

  // ── Select row / editor ────────────────────────────────────────────────────

  const selectRow = useCallback(
    (id: string, row: Record<string, unknown>) => {
      setSelectedDocId(id);
      const { _id: _, ...doc } = row;
      setEditorJson(JSON.stringify(doc, null, 2));
      setEditorMode("view");
      setEditorError(null);
    },
    [],
  );

  const openNewDoc = useCallback(() => {
    setSelectedDocId(null);
    setEditorJson("{\n  \n}");
    setEditorMode("new");
    setEditorError(null);
  }, []);

  // ── Save (insert / patch) ──────────────────────────────────────────────────

  const saveDoc = useCallback(async () => {
    if (!client || !selectedColl) return;
    let doc: Record<string, unknown>;
    try {
      doc = JSON.parse(editorJson) as Record<string, unknown>;
    } catch {
      setEditorError("Invalid JSON");
      return;
    }
    setEditorError(null);
    setSaving(true);
    try {
      if (editorMode === "new") {
        const { id, result } = await client.insertDoc(selectedColl, doc);
        setStatus(`Inserted doc ${id} (${result})`);
      } else if (editorMode === "edit" && selectedDocId) {
        const result = await client.patchDoc(selectedColl, selectedDocId, doc);
        setStatus(`Patched doc ${selectedDocId} (${result})`);
      }
      const filter = parseFilter(filterText);
      await loadDocs(selectedColl, typeof filter === "string" ? null : filter);
      setEditorMode("view");
    } catch (e) {
      setEditorError(String(e));
    } finally {
      setSaving(false);
    }
  }, [client, selectedColl, editorMode, editorJson, selectedDocId, filterText, loadDocs]);

  // ── Delete doc ─────────────────────────────────────────────────────────────

  const deleteDoc = useCallback(async () => {
    if (!client || !selectedColl || !selectedDocId) return;
    if (!confirm(`Delete document ${selectedDocId}?`)) return;
    try {
      await client.deleteDoc(selectedColl, selectedDocId);
      setStatus(`Deleted doc ${selectedDocId}`);
      setSelectedDocId(null);
      setEditorJson("");
      setEditorMode("view");
      const filter = parseFilter(filterText);
      await loadDocs(selectedColl, typeof filter === "string" ? null : filter);
    } catch (e) {
      setStatus(`Error: ${String(e)}`);
    }
  }, [client, selectedColl, selectedDocId, filterText, loadDocs]);

  // ── Refresh index states ───────────────────────────────────────────────────

  const refreshIndexStates = useCallback(async () => {
    if (!client || !selectedColl) return;
    try {
      const idxs = await client.listIndexes(selectedColl);
      setIndexes(idxs);
    } catch (e) {
      setStatus(`Error: ${String(e)}`);
    }
  }, [client, selectedColl]);

  // ── Keyboard shortcuts ─────────────────────────────────────────────────────

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
        void applyFilter();
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [applyFilter]);

  // ── Render ─────────────────────────────────────────────────────────────────

  const connected = client?.connected ?? false;
  const flatRows  = flattenRows(rows);

  return (
    <div className="fixed inset-0 flex flex-col bg-background text-foreground overflow-hidden">
      {/* ── Header ─────────────────────────────────────────────────────────── */}
      <header className="flex items-center gap-3 px-3 py-2 border-b border-border shrink-0 bg-muted/30">
        <Database className="size-4 text-primary shrink-0" />
        <span className="font-semibold text-sm tracking-tight">experimental-db studio</span>

        <div className="flex-1 flex items-center gap-2 max-w-sm ml-4">
          <Input
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter" && !connected) void connect(); }}
            disabled={connected || connecting}
            placeholder="ws://127.0.0.1:3000/ws"
            className="h-7 text-xs font-mono"
          />
          {connected ? (
            <Button size="sm" variant="outline" onClick={disconnect} className="h-7 text-xs shrink-0">
              <WifiOff className="size-3" /> Disconnect
            </Button>
          ) : (
            <Button size="sm" onClick={() => void connect()} disabled={connecting} className="h-7 text-xs shrink-0">
              {connecting ? <Loader2 className="size-3 animate-spin" /> : <Wifi className="size-3" />}
              Connect
            </Button>
          )}
        </div>

        <div className="flex items-center gap-1.5 ml-auto">
          {connected ? (
            <span className="flex items-center gap-1 text-xs text-green-600 dark:text-green-400">
              <Circle className="size-2 fill-current" /> Connected
            </span>
          ) : (
            <span className="flex items-center gap-1 text-xs text-muted-foreground">
              <Circle className="size-2" /> Disconnected
            </span>
          )}
        </div>
      </header>

      {connError && (
        <div className="px-3 py-1.5 text-xs bg-destructive/10 text-destructive border-b border-destructive/20 flex items-center gap-2">
          <AlertCircle className="size-3 shrink-0" />
          {connError}
          <button className="ml-auto" onClick={() => setConnError(null)}><X className="size-3" /></button>
        </div>
      )}

      {/* ── Main content ────────────────────────────────────────────────────── */}
      <div className="flex flex-1 overflow-hidden">
        {/* ── Sidebar ──────────────────────────────────────────────────────── */}
        <aside className="w-52 shrink-0 border-r border-border flex flex-col overflow-hidden bg-muted/10">
          {/* Collections header */}
          <div className="flex items-center justify-between px-2 py-1.5 border-b border-border">
            <span className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Collections</span>
            <div className="flex items-center gap-1">
              <button
                onClick={() => void reloadCollections()}
                disabled={!connected}
                title="Refresh collections"
                className="rounded p-0.5 hover:bg-accent disabled:opacity-40"
              >
                <RefreshCw className="size-3" />
              </button>
              <button
                onClick={() => setShowNewColl((v) => !v)}
                disabled={!connected}
                title="New collection"
                className="rounded p-0.5 hover:bg-accent disabled:opacity-40"
              >
                <Plus className="size-3" />
              </button>
            </div>
          </div>

          {/* New collection form */}
          {showNewColl && (
            <div className="flex gap-1 px-2 py-1.5 border-b border-border">
              <Input
                autoFocus
                value={newCollName}
                onChange={(e) => setNewCollName(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter") void createCollection(); if (e.key === "Escape") setShowNewColl(false); }}
                placeholder="collection name"
                className="h-6 text-xs"
              />
              <button onClick={() => void createCollection()} className="shrink-0 text-xs px-1.5 bg-primary text-primary-foreground rounded hover:bg-primary/90">
                Add
              </button>
            </div>
          )}

          {/* Collection list */}
          <div className="flex-1 overflow-y-auto py-1">
            {collections.length === 0 && connected && (
              <p className="text-xs text-muted-foreground px-3 py-2">No collections</p>
            )}
            {!connected && (
              <p className="text-xs text-muted-foreground px-3 py-2">Not connected</p>
            )}
            {collections.map((name) => (
              <button
                key={name}
                onClick={() => void selectCollection(name)}
                className={[
                  "w-full text-left flex items-center gap-1.5 px-2 py-1 text-xs hover:bg-accent transition-colors",
                  selectedColl === name ? "bg-primary/10 text-primary font-medium" : "text-foreground",
                ].join(" ")}
              >
                <ChevronRight className={["size-3 shrink-0 transition-transform", selectedColl === name ? "rotate-90 text-primary" : "text-muted-foreground"].join(" ")} />
                <span className="truncate">{name}</span>
              </button>
            ))}
          </div>

          {/* Indexes section (shown when a collection is selected) */}
          {selectedColl && (
            <div className="border-t border-border">
              <div className="flex items-center justify-between px-2 py-1.5">
                <button
                  onClick={() => setShowIndexes((v) => !v)}
                  className="flex items-center gap-1 text-xs font-semibold uppercase tracking-wide text-muted-foreground hover:text-foreground"
                >
                  <ChevronRight className={["size-3 transition-transform", showIndexes ? "rotate-90" : ""].join(" ")} />
                  Indexes
                </button>
                <div className="flex items-center gap-1">
                  <button
                    onClick={() => void refreshIndexStates()}
                    title="Refresh index states"
                    className="rounded p-0.5 hover:bg-accent"
                  >
                    <RefreshCw className="size-3" />
                  </button>
                  <button
                    onClick={() => { setShowIndexes(true); setShowNewIdx((v) => !v); }}
                    title="Create index"
                    className="rounded p-0.5 hover:bg-accent"
                  >
                    <Plus className="size-3" />
                  </button>
                </div>
              </div>

              {showIndexes && (
                <div className="pb-1">
                  {/* New index form */}
                  {showNewIdx && (
                    <div className="flex flex-col gap-1 px-2 pb-1.5">
                      <Input
                        autoFocus
                        value={newIdxField}
                        onChange={(e) => setNewIdxField(e.target.value)}
                        onKeyDown={(e) => { if (e.key === "Enter") void createIndex(); if (e.key === "Escape") setShowNewIdx(false); }}
                        placeholder="field.path"
                        className="h-6 text-xs"
                      />
                      <div className="flex items-center gap-2">
                        <label className="flex items-center gap-1 text-xs cursor-pointer">
                          <input
                            type="checkbox"
                            checked={newIdxUnique}
                            onChange={(e) => setNewIdxUnique(e.target.checked)}
                            className="size-3"
                          />
                          unique
                        </label>
                        <button
                          onClick={() => void createIndex()}
                          className="ml-auto text-xs px-1.5 py-0.5 bg-primary text-primary-foreground rounded hover:bg-primary/90"
                        >
                          Create
                        </button>
                      </div>
                    </div>
                  )}

                  {indexes.length === 0 ? (
                    <p className="text-xs text-muted-foreground px-3 py-1">No indexes</p>
                  ) : (
                    indexes.map((idx) => (
                      <div key={idx.index_id} className="px-3 py-0.5 flex items-center gap-2">
                        <div className="flex-1 min-w-0">
                          <span className="text-xs font-mono truncate">{idx.field}</span>
                          {idx.unique && (
                            <span className="ml-1 text-[10px] text-muted-foreground">unique</span>
                          )}
                        </div>
                        <StatusBadge state={idx.state} />
                      </div>
                    ))
                  )}
                </div>
              )}
            </div>
          )}
        </aside>

        {/* ── Main panel ───────────────────────────────────────────────────── */}
        <div className="flex flex-1 overflow-hidden flex-col">
          {/* Toolbar */}
          <div className="flex items-center gap-2 px-2 py-1.5 border-b border-border shrink-0 bg-muted/5">
            {selectedColl ? (
              <>
                {/* Filter input */}
                <div className="flex items-center gap-1 flex-1 max-w-md">
                  <Search className="size-3 text-muted-foreground shrink-0 ml-1" />
                  <Input
                    value={filterText}
                    onChange={(e) => { setFilterText(e.target.value); setFilterError(null); }}
                    onKeyDown={(e) => { if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) void applyFilter(); }}
                    placeholder='Filter JSON, e.g. {"gt":["price",10]}  ⌘↵ to apply'
                    className={["h-7 text-xs font-mono border-0 shadow-none bg-transparent px-1", filterError ? "text-destructive" : ""].join(" ")}
                  />
                  {filterText && (
                    <button
                      onClick={() => { setFilterText(""); setFilterError(null); void loadDocs(selectedColl, null); }}
                      className="shrink-0"
                    >
                      <X className="size-3 text-muted-foreground hover:text-foreground" />
                    </button>
                  )}
                  <Button size="sm" variant="outline" onClick={() => void applyFilter()} className="h-7 text-xs shrink-0">
                    Run
                  </Button>
                </div>

                {filterError && (
                  <span className="text-xs text-destructive">{filterError}</span>
                )}

                <div className="flex items-center gap-1 ml-auto">
                  {/* Refresh */}
                  <Button
                    size="icon-sm"
                    variant="ghost"
                    title="Refresh"
                    onClick={() => void loadDocs(selectedColl, parseFilter(filterText) instanceof Object ? (parseFilter(filterText) as Filter) : null)}
                  >
                    <RotateCcw className={["size-3", loading ? "animate-spin" : ""].join(" ")} />
                  </Button>

                  {/* Subscribe toggle */}
                  <Button
                    size="sm"
                    variant={subscribed ? "default" : "outline"}
                    onClick={() => void toggleSubscription()}
                    className="h-7 text-xs"
                    title={subscribed ? "Stop live subscription" : "Start live subscription"}
                  >
                    {subscribed ? <Zap className="size-3 text-yellow-400" /> : <ZapOff className="size-3" />}
                    {subscribed ? "Live" : "Subscribe"}
                  </Button>

                  {/* New doc */}
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={openNewDoc}
                    className="h-7 text-xs"
                  >
                    <FilePlus className="size-3" /> New Doc
                  </Button>

                  {/* Delete collection */}
                  <Button
                    size="icon-sm"
                    variant="ghost"
                    title={`Delete collection "${selectedColl}"`}
                    onClick={() => void deleteCollection()}
                    className="text-destructive hover:text-destructive hover:bg-destructive/10"
                  >
                    <Trash2 className="size-3" />
                  </Button>
                </div>
              </>
            ) : (
              <span className="text-xs text-muted-foreground px-1">
                {connected ? "Select a collection from the sidebar" : "Connect to a database to get started"}
              </span>
            )}
          </div>

          {/* Grid + editor */}
          <div className="flex flex-1 overflow-hidden">
            {/* Data grid */}
            <div className="flex-1 overflow-hidden flex flex-col">
              {loadError ? (
                <div className="flex items-center gap-2 p-4 text-sm text-destructive">
                  <AlertCircle className="size-4 shrink-0" />
                  {loadError}
                </div>
              ) : loading ? (
                <div className="flex items-center justify-center flex-1 gap-2 text-sm text-muted-foreground">
                  <Loader2 className="size-4 animate-spin" /> Loading…
                </div>
              ) : selectedColl ? (
                <DataGrid
                  columns={columns}
                  rows={flatRows}
                  selectedId={selectedDocId}
                  onSelectRow={selectRow}
                  onDoubleClickRow={(id, row) => {
                    selectRow(id, row);
                    setEditorMode("edit");
                  }}
                  emptyMessage={`No documents in "${selectedColl}"`}
                />
              ) : (
                <div className="flex-1 flex items-center justify-center text-sm text-muted-foreground">
                  <div className="text-center space-y-2">
                    <Database className="size-10 mx-auto text-muted-foreground/40" />
                    <p>Select a collection to browse documents</p>
                  </div>
                </div>
              )}
            </div>

            {/* Document editor panel */}
            {(editorMode !== "view" || selectedDocId) && (
              <div className="w-80 shrink-0 border-l border-border flex flex-col bg-muted/5 overflow-hidden">
                {/* Panel header */}
                <div className="flex items-center gap-2 px-3 py-1.5 border-b border-border shrink-0">
                  <span className="text-xs font-semibold flex-1 truncate font-mono">
                    {editorMode === "new"
                      ? "New Document"
                      : `${selectedDocId?.slice(0, 16)}…`}
                  </span>
                  {editorMode === "view" && selectedDocId && (
                    <Button
                      size="icon-sm"
                      variant="ghost"
                      onClick={() => setEditorMode("edit")}
                      title="Edit document"
                      className="text-xs"
                    >
                      ✏️
                    </Button>
                  )}
                  <button
                    onClick={() => {
                      setSelectedDocId(null);
                      setEditorJson("");
                      setEditorMode("view");
                    }}
                    className="rounded p-0.5 hover:bg-accent"
                  >
                    <X className="size-3" />
                  </button>
                </div>

                {/* Doc ID display (for existing docs) */}
                {selectedDocId && editorMode !== "new" && (
                  <div className="px-3 py-1 border-b border-border shrink-0">
                    <span className="text-[10px] text-muted-foreground">_id</span>
                    <p className="text-xs font-mono break-all">{selectedDocId}</p>
                  </div>
                )}

                {/* JSON editor */}
                <div className="flex-1 overflow-hidden p-0">
                  <Textarea
                    value={editorJson}
                    onChange={(e) => setEditorJson(e.target.value)}
                    readOnly={editorMode === "view"}
                    className={[
                      "w-full h-full resize-none rounded-none border-0 font-mono text-xs p-2 bg-transparent focus-visible:ring-0 focus-visible:ring-offset-0",
                      editorMode === "view" ? "cursor-default select-text" : "",
                    ].join(" ")}
                    spellCheck={false}
                  />
                </div>

                {/* Error */}
                {editorError && (
                  <div className="px-3 py-1 text-xs text-destructive border-t border-destructive/20 flex items-center gap-1">
                    <AlertCircle className="size-3 shrink-0" />
                    {editorError}
                  </div>
                )}

                {/* Actions */}
                {editorMode !== "view" && (
                  <div className="flex gap-2 px-3 py-2 border-t border-border shrink-0">
                    <Button
                      size="sm"
                      onClick={() => void saveDoc()}
                      disabled={saving}
                      className="h-7 text-xs flex-1"
                    >
                      {saving ? <Loader2 className="size-3 animate-spin" /> : null}
                      {editorMode === "new" ? "Insert" : "Save"}
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => { setEditorMode("view"); setEditorError(null); }}
                      className="h-7 text-xs"
                    >
                      Cancel
                    </Button>
                  </div>
                )}

                {/* Delete button for existing docs */}
                {editorMode === "view" && selectedDocId && (
                  <div className="px-3 py-2 border-t border-border shrink-0 flex gap-2">
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => setEditorMode("edit")}
                      className="h-7 text-xs flex-1"
                    >
                      Edit
                    </Button>
                    <Button
                      size="sm"
                      variant="destructive"
                      onClick={() => void deleteDoc()}
                      className="h-7 text-xs"
                    >
                      <Trash2 className="size-3" /> Delete
                    </Button>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* ── Status bar ─────────────────────────────────────────────────────── */}
      <div className="flex items-center px-3 py-0.5 border-t border-border bg-muted/20 shrink-0">
        {subscribed && (
          <span className="flex items-center gap-1 mr-3 text-[10px] text-yellow-600 dark:text-yellow-400">
            <Zap className="size-2.5 fill-current" /> Live
          </span>
        )}
        <span className="text-[11px] text-muted-foreground">{status}</span>
        {selectedColl && (
          <span className="ml-auto text-[11px] text-muted-foreground font-mono">{selectedColl}</span>
        )}
      </div>
    </div>
  );
}

export default App;
