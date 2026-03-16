use dioxus::prelude::*;

use crate::state::AppState;

#[component]
pub fn ConfigViewerModule() -> Element {
    let state = use_context::<AppState>();
    let engine = state.engine.read().clone();

    let Some(engine) = engine else {
        return rsx! {
            div { class: "main-content",
                div { class: "empty-state", "No database open" }
            }
        };
    };

    let config = engine.l6.config();
    let tx = &config.transaction;

    let mem_mb = config.memory_budget / (1024 * 1024);
    let max_doc_mb = config.max_doc_size / (1024 * 1024);
    let wal_mb = config.wal_segment_size / (1024 * 1024);
    let ckpt_mb = config.checkpoint_wal_threshold / (1024 * 1024);
    let ckpt_s = config.checkpoint_interval.as_secs();
    let vac_s = config.vacuum_interval.as_secs();
    let idle_s = tx.idle_timeout.as_secs();
    let max_life_s = tx.max_lifetime.as_secs();
    let scan_mb = tx.max_scanned_bytes / (1024 * 1024);

    rsx! {
        div { class: "main-content",
            div { class: "card",
                h3 { "Database Configuration" }
                table { class: "kv-table",
                    tbody {
                        tr { td { "Page Size" } td { "{config.page_size} bytes" } }
                        tr { td { "Memory Budget" } td { "{mem_mb} MB" } }
                        tr { td { "Max Document Size" } td { "{max_doc_mb} MB" } }
                        tr { td { "External Threshold" } td { "{config.external_threshold} bytes" } }
                        tr { td { "WAL Segment Size" } td { "{wal_mb} MB" } }
                        tr { td { "Checkpoint WAL Threshold" } td { "{ckpt_mb} MB" } }
                        tr { td { "Checkpoint Interval" } td { "{ckpt_s}s" } }
                        tr { td { "Vacuum Interval" } td { "{vac_s}s" } }
                    }
                }
            }

            div { class: "card",
                h3 { "Transaction Configuration" }
                table { class: "kv-table",
                    tbody {
                        tr { td { "Idle Timeout" } td { "{idle_s}s" } }
                        tr { td { "Max Lifetime" } td { "{max_life_s}s" } }
                        tr { td { "Max Intervals" } td { "{tx.max_intervals}" } }
                        tr { td { "Max Scanned Bytes" } td { "{scan_mb} MB" } }
                        tr { td { "Max Scanned Docs" } td { "{tx.max_scanned_docs}" } }
                    }
                }
            }
        }
    }
}
