use std::path::PathBuf;
use std::sync::Arc;

use dioxus::prelude::*;

use crate::engine::DbHandle;
use crate::state::{AppState, LayerTab, OperationResult};

#[component]
pub fn Toolbar() -> Element {
    let mut state = use_context::<AppState>();
    let db_open = state.is_open();
    let write_enabled = *state.write_enabled.read();
    let dirty = *state.dirty_page_count.read();

    // Db info line
    let info_text = if let Some(db) = state.db.read().as_ref() {
        let fh = db.read_file_header();
        format!(
            "{} ({} B pages, {} pages)",
            db.path,
            fh.page_size.get(),
            fh.page_count.get()
        )
    } else {
        "No database open".to_string()
    };

    rsx! {
        div { class: "toolbar",
            div { class: "toolbar-group",
                button {
                    class: "btn",
                    onclick: move |_| {
                        spawn(async move {
                            if let Some(path) = pick_database_dir().await {
                                match tokio::task::spawn_blocking(move || DbHandle::open(&path)).await {
                                    Ok(Ok(handle)) => {
                                        state.db.set(Some(Arc::new(handle)));
                                        state.active_tab.set(LayerTab::Overview);
                                        state.breadcrumb.set(vec!["Database".to_string(), "Overview".to_string()]);
                                        state.write_enabled.set(false);
                                        state.last_result.set(Some(OperationResult::Success("Database opened".into())));
                                    }
                                    Ok(Err(e)) => {
                                        state.last_result.set(Some(OperationResult::Error(format!("Failed to open: {e}"))));
                                    }
                                    Err(e) => {
                                        state.last_result.set(Some(OperationResult::Error(format!("Task error: {e}"))));
                                    }
                                }
                            }
                        });
                    },
                    "Open DB..."
                }
                button {
                    class: "btn",
                    onclick: move |_| {
                        spawn(async move {
                            if let Some(path) = pick_new_database_dir().await {
                                match tokio::task::spawn_blocking(move || DbHandle::open(&path)).await {
                                    Ok(Ok(handle)) => {
                                        state.db.set(Some(Arc::new(handle)));
                                        state.active_tab.set(LayerTab::Overview);
                                        state.breadcrumb.set(vec!["Database".to_string(), "Overview".to_string()]);
                                        state.write_enabled.set(true);
                                        state.last_result.set(Some(OperationResult::Success("New database created".into())));
                                    }
                                    Ok(Err(e)) => {
                                        state.last_result.set(Some(OperationResult::Error(format!("Failed to create: {e}"))));
                                    }
                                    Err(e) => {
                                        state.last_result.set(Some(OperationResult::Error(format!("Task error: {e}"))));
                                    }
                                }
                            }
                        });
                    },
                    "New DB..."
                }
                if db_open {
                    button {
                        class: "btn",
                        onclick: move |_| {
                            let db = state.db.read().clone();
                            spawn(async move {
                                if let Some(db) = db {
                                    let _ = db.close().await;
                                }
                                state.db.set(None);
                                state.write_enabled.set(false);
                                state.breadcrumb.set(vec!["Database".to_string()]);
                                state.last_result.set(Some(OperationResult::Success("Database closed".into())));
                            });
                        },
                        "Close"
                    }
                }
            }

            div { class: "toolbar-separator" }

            div { class: "toolbar-info", "{info_text}" }

            if db_open {
                div { class: "toolbar-group",
                    button {
                        class: "btn btn-action",
                        disabled: !write_enabled,
                        onclick: move |_| {
                            let db = state.db.read().clone();
                            spawn(async move {
                                if let Some(db) = db {
                                    match db.checkpoint().await {
                                        Ok(()) => {
                                            state.notify_mutation();
                                            state.last_result.set(Some(OperationResult::Success("Checkpoint complete".into())));
                                        }
                                        Err(e) => {
                                            state.last_result.set(Some(OperationResult::Error(format!("Checkpoint failed: {e}"))));
                                        }
                                    }
                                }
                            });
                        },
                        "Checkpoint"
                    }
                    span {
                        class: "toolbar-info",
                        "Dirty: {dirty}"
                    }
                }

                div { class: "toolbar-separator" }

                // RW Lock badge
                {
                    let badge_class = if write_enabled { "rw-badge unlocked" } else { "rw-badge locked" };
                    let badge_text = if write_enabled { "READ-WRITE" } else { "READ-ONLY" };
                    rsx! {
                        div {
                            class: "{badge_class}",
                            onclick: move |_| {
                                if write_enabled {
                                    // Lock immediately
                                    state.write_enabled.set(false);
                                } else {
                                    // Unlock (should show confirm dialog; for now just toggle)
                                    state.write_enabled.set(true);
                                }
                            },
                            "{badge_text}"
                        }
                    }
                }
            }
        }
    }
}

async fn pick_database_dir() -> Option<PathBuf> {
    let handle = rfd::AsyncFileDialog::new()
        .set_title("Open Database Directory")
        .pick_folder()
        .await?;
    Some(handle.path().to_path_buf())
}

async fn pick_new_database_dir() -> Option<PathBuf> {
    // User picks a parent folder, then we append a "new.exdb" subdir.
    // If the picked folder is already empty, use it directly.
    let handle = rfd::AsyncFileDialog::new()
        .set_title("Choose Directory for New Database")
        .pick_folder()
        .await?;
    let path = handle.path().to_path_buf();

    // If the directory is empty (or doesn't have data.db), use it directly as the DB dir.
    // Otherwise create a "new.exdb" subdirectory.
    if !path.join("data.db").exists() {
        Some(path)
    } else {
        let mut candidate = path.join("new.exdb");
        let mut i = 1;
        while candidate.exists() {
            candidate = path.join(format!("new_{i}.exdb"));
            i += 1;
        }
        Some(candidate)
    }
}
