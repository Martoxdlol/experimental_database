use dioxus::prelude::*;

use crate::state::{AppState, DocstoreTool, LayerTab, QueryTool, StorageTool};

#[component]
pub fn Sidebar() -> Element {
    let state = use_context::<AppState>();
    let active_tab = *state.active_tab.read();
    let db_open = state.is_open();

    // Overview and Console are full-width — no sidebar
    if !db_open || matches!(active_tab, LayerTab::Overview | LayerTab::Console) {
        return rsx! {};
    }

    match active_tab {
        LayerTab::Storage => rsx! { StorageSidebar {} },
        LayerTab::Docstore => rsx! { DocstoreSidebar {} },
        LayerTab::Query => rsx! { QuerySidebar {} },
        _ => rsx! {},
    }
}

#[component]
fn StorageSidebar() -> Element {
    let mut state = use_context::<AppState>();
    let active = *state.storage_tool.read();

    rsx! {
        div { class: "sidebar",
            for tool in StorageTool::ALL {
                {
                    let t = *tool;
                    let is_active = t == active;
                    let class = if is_active { "sidebar-item active" } else { "sidebar-item" };
                    rsx! {
                        div {
                            class: "{class}",
                            onclick: move |_| {
                                state.storage_tool.set(t);
                                state.breadcrumb.set(vec![
                                    "Database".to_string(),
                                    "L2 Storage".to_string(),
                                    t.label().to_string(),
                                ]);
                            },
                            div { class: "sidebar-icon", "{t.icon()}" }
                            "{t.label()}"
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn DocstoreSidebar() -> Element {
    let mut state = use_context::<AppState>();
    let active = *state.docstore_tool.read();

    rsx! {
        div { class: "sidebar",
            for tool in DocstoreTool::ALL {
                {
                    let t = *tool;
                    let is_active = t == active;
                    let class = if is_active { "sidebar-item active" } else { "sidebar-item" };
                    rsx! {
                        div {
                            class: "{class}",
                            onclick: move |_| {
                                state.docstore_tool.set(t);
                                state.breadcrumb.set(vec![
                                    "Database".to_string(),
                                    "L3 Docstore".to_string(),
                                    t.label().to_string(),
                                ]);
                            },
                            div { class: "sidebar-icon", "{t.icon()}" }
                            "{t.label()}"
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn QuerySidebar() -> Element {
    let mut state = use_context::<AppState>();
    let active = *state.query_tool.read();

    rsx! {
        div { class: "sidebar",
            for tool in QueryTool::ALL {
                {
                    let t = *tool;
                    let is_active = t == active;
                    let class = if is_active { "sidebar-item active" } else { "sidebar-item" };
                    rsx! {
                        div {
                            class: "{class}",
                            onclick: move |_| {
                                state.query_tool.set(t);
                                state.breadcrumb.set(vec![
                                    "Database".to_string(),
                                    "L4 Query".to_string(),
                                    t.label().to_string(),
                                ]);
                            },
                            div { class: "sidebar-icon", "{t.icon()}" }
                            "{t.label()}"
                        }
                    }
                }
            }
        }
    }
}
