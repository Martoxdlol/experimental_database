use dioxus::prelude::*;

use crate::state::AppState;

#[component]
pub fn VacuumModule() -> Element {
    let state = use_context::<AppState>();
    let db = state.db.read().clone();

    let Some(db) = db else {
        return rsx! { div { class: "empty-state", "No database open" } };
    };

    let fh = db.read_file_header();
    let visible_ts = fh.visible_ts.get();

    let db_for_stats = db.clone();
    let stats = use_resource(move || {
        let _rev = *state.revision.read();
        let db = db_for_stats.clone();
        async move {
            tokio::task::spawn_blocking(move || db.vacuum_stats())
                .await
                .ok()
                .and_then(|r| r.ok())
        }
    });

    rsx! {
        div { class: "main-content",
            h2 { style: "margin-bottom: 16px;", "Vacuum & MVCC Stats" }

            // Timestamp state card
            div { class: "card",
                div { class: "card-header", "Timestamp State" }
                div { class: "card-body",
                    table { class: "kv-table",
                        tr {
                            td { "visible_ts" }
                            td { span { class: "kv-value", "{visible_ts}" } }
                        }
                        tr {
                            td { "Meaning" }
                            td {
                                span { style: "font-size: 11px; color: var(--text-secondary);",
                                    "Versions with superseding_ts > {visible_ts} cannot be vacuumed. "
                                    "Commits with ts > {visible_ts} may be rolled back on crash."
                                }
                            }
                        }
                    }
                }
            }

            // Per-collection stats
            if let Some(Some(collections)) = stats.read().as_ref() {
                for s in collections {
                    div { class: "card",
                        div { class: "card-header", "{s.collection_name}" }
                        div { class: "card-body",
                            table { class: "kv-table",
                                tr {
                                    td { "Raw B-tree entries" }
                                    td { span { class: "kv-value", "{s.total_raw_entries}" } }
                                }
                                tr {
                                    td { "Visible documents" }
                                    td { span { class: "kv-value", "{s.visible_docs}" } }
                                }
                                tr {
                                    td { "Tombstones" }
                                    td {
                                        if s.tombstones > 0 {
                                            span { class: "tombstone-badge", "{s.tombstones}" }
                                        } else {
                                            span { class: "kv-value", "0" }
                                        }
                                    }
                                }
                                tr {
                                    td { "Old versions (reclaimable)" }
                                    td { span { class: "kv-value", "{s.version_overhead}" } }
                                }
                            }

                            // Visual bar
                            if s.total_raw_entries > 0 {
                                {
                                    let total = s.total_raw_entries as f64;
                                    let vis_pct = (s.visible_docs as f64 / total) * 100.0;
                                    let tomb_pct = (s.tombstones as f64 / total) * 100.0;
                                    let old_pct = (s.version_overhead as f64 / total) * 100.0;
                                    rsx! {
                                        div { style: "margin-top: 12px;",
                                            div { class: "space-bar",
                                                div {
                                                    class: "space-bar-segment",
                                                    style: "background: var(--checksum-ok); width: {vis_pct:.0}%;",
                                                    if vis_pct > 10.0 { "live" }
                                                }
                                                div {
                                                    class: "space-bar-segment",
                                                    style: "background: var(--page-deleted); width: {tomb_pct:.0}%;",
                                                    if tomb_pct > 10.0 { "tomb" }
                                                }
                                                div {
                                                    class: "space-bar-segment",
                                                    style: "background: var(--page-free); width: {old_pct:.0}%;",
                                                    if old_pct > 10.0 { "old" }
                                                }
                                            }
                                            div { style: "display: flex; gap: 16px; margin-top: 4px; font-size: 10px; color: var(--text-secondary);",
                                                span { "\u{25CF} live ({vis_pct:.0}%)" }
                                                span { style: "color: var(--page-deleted);", "\u{25CF} tombstones ({tomb_pct:.0}%)" }
                                                span { "\u{25CF} old versions ({old_pct:.0}%)" }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if collections.is_empty() {
                    div { style: "color: var(--text-secondary);", "No collections found" }
                }
            } else {
                div { "Loading stats..." }
            }
        }
    }
}
