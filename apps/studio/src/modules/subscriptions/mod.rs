use dioxus::prelude::*;

use crate::state::AppState;

#[component]
pub fn SubscriptionsModule() -> Element {
    let state = use_context::<AppState>();
    let _rev = *state.revision.read();

    let engine = state.engine.read().clone();
    let collections = engine.as_ref().map(|e| e.l6.list_collections()).unwrap_or_default();

    let events: Signal<Vec<String>> = use_signal(Vec::new);
    let active = use_signal(|| false);

    rsx! {
        div { class: "main-content",
            div { class: "card",
                h3 { "Live Subscriptions" }
                p { style: "color: var(--text-secondary);",
                    "Subscribe to query invalidation events. When data changes that affects "
                    "your subscribed query, you'll receive a notification here."
                }
                div { class: "form-row",
                    span {
                        class: if *active.read() { "index-badge ready" } else { "index-badge dropping" },
                        if *active.read() { "Active" } else { "Inactive" }
                    }
                }
            }

            div { class: "card",
                h4 { "Available Collections" }
                if collections.is_empty() {
                    div { class: "empty-state", "No collections to subscribe to" }
                } else {
                    table { class: "data-table",
                        thead {
                            tr {
                                th { "Collection" }
                                th { "Documents" }
                            }
                        }
                        tbody {
                            for col in &collections {
                                tr {
                                    td { class: "mono", "{col.name}" }
                                    td { "{col.doc_count}" }
                                }
                            }
                        }
                    }
                }
            }

            div { class: "card",
                h4 { "Event Log ({events.read().len()})" }
                if events.read().is_empty() {
                    div { class: "empty-state", "No events yet" }
                } else {
                    div { class: "event-log",
                        for (i, event) in events.read().iter().enumerate() {
                            div { class: "event-entry", key: "{i}", "{event}" }
                        }
                    }
                }
            }
        }
    }
}
