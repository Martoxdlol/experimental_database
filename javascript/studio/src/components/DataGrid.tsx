/**
 * Spreadsheet-style data grid.
 * Renders via divs (not <table>). Sticky header. No outer margin/padding.
 */
import { useLayoutEffect, useRef, useState } from "react";

// ── Types ─────────────────────────────────────────────────────────────────────

export interface GridColumn {
  key: string;
  label: string;
  width?: number; // px, defaults to 160
}

export interface DataGridProps {
  columns: GridColumn[];
  rows: Record<string, unknown>[];
  selectedId?: string | null;
  idKey?: string; // defaults to "_id"
  onSelectRow?: (id: string, row: Record<string, unknown>) => void;
  onDoubleClickRow?: (id: string, row: Record<string, unknown>) => void;
  emptyMessage?: string;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function cellText(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

// ── DataGrid ──────────────────────────────────────────────────────────────────

export function DataGrid({
  columns,
  rows,
  selectedId,
  idKey = "_id",
  onSelectRow,
  onDoubleClickRow,
  emptyMessage = "No documents",
}: DataGridProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [scrollbarW, setScrollbarW] = useState(0);

  // Measure scrollbar width so header aligns perfectly with body
  useLayoutEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    setScrollbarW(el.offsetWidth - el.clientWidth);
  });

  const totalWidth = columns.reduce((s, c) => s + (c.width ?? 160), 0);

  const headerCells = columns.map((col) => (
    <div
      key={col.key}
      style={{ width: col.width ?? 160, minWidth: col.width ?? 160, flexShrink: 0 }}
      className="px-2 py-1 text-xs font-semibold text-muted-foreground uppercase tracking-wide border-r border-border last:border-r-0 truncate select-none bg-muted"
    >
      {col.label}
    </div>
  ));

  return (
    <div className="flex flex-col h-full w-full overflow-hidden border border-border rounded-none">
      {/* Sticky header */}
      <div
        className="flex border-b border-border shrink-0"
        style={{ paddingRight: scrollbarW }}
      >
        <div className="flex" style={{ minWidth: totalWidth }}>
          {headerCells}
        </div>
      </div>

      {/* Scrollable body */}
      <div ref={containerRef} className="flex-1 overflow-auto">
        {rows.length === 0 ? (
          <div className="flex items-center justify-center h-full text-sm text-muted-foreground py-8">
            {emptyMessage}
          </div>
        ) : (
          <div style={{ minWidth: totalWidth }}>
            {rows.map((row, idx) => {
              const id = String(row[idKey] ?? idx);
              const isSelected = id === selectedId;
              return (
                <div
                  key={id}
                  className={[
                    "flex border-b border-border last:border-b-0 cursor-pointer",
                    isSelected
                      ? "bg-primary/10"
                      : idx % 2 === 0
                      ? "bg-background hover:bg-muted/50"
                      : "bg-muted/20 hover:bg-muted/50",
                  ].join(" ")}
                  onClick={() => onSelectRow?.(id, row)}
                  onDoubleClick={() => onDoubleClickRow?.(id, row)}
                >
                  {columns.map((col) => (
                    <div
                      key={col.key}
                      style={{ width: col.width ?? 160, minWidth: col.width ?? 160, flexShrink: 0 }}
                      className="px-2 py-1 text-xs border-r border-border last:border-r-0 truncate font-mono"
                      title={cellText(row[col.key])}
                    >
                      {cellText(row[col.key])}
                    </div>
                  ))}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
