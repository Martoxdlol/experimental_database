use std::path::PathBuf;
use std::sync::Arc;

use dioxus::prelude::*;

use crate::engine::EngineHandle;
use crate::state::{AppState, NavSection, OperationResult};

#[component]
pub fn Toolbar() -> Element {
    let mut state = use_context::<AppState>();
    let db_open = state.is_open();
    let write_enabled = *state.write_enabled.read();
    let dirty = *state.dirty_page_count.read();

    // Db info line
    let engine = state.engine.read().clone();
    let info_resource = use_resource(move || {
        let _rev = *state.revision.read();
        let engine = engine.clone();
        async move {
            if let Some(engine) = engine {
                let fh = engine.storage.read_file_header().await;
                format!(
                    "{} ({} B pages, {} pages)",
                    engine.path,
                    fh.page_size.get(),
                    fh.page_count.get()
                )
            } else {
                "No database open".to_string()
            }
        }
    });
    let info_text = info_resource
        .read()
        .clone()
        .unwrap_or_else(|| "No database open".to_string());

    rsx! {
        div { class: "toolbar",
            div { class: "toolbar-group",
                button {
                    class: "btn",
                    onclick: move |_| {
                        spawn(async move {
                            if let Some(path) = pick_database_dir().await {
                                match EngineHandle::open(&path).await {
                                    Ok(handle) => {
                                        let storage = Arc::clone(&handle.storage);
                                        let handle = Arc::new(handle);
                                        state.engine.set(Some(handle));
                                        state.db.set(Some(storage));
                                        state.nav.set(NavSection::Dashboard);
                                        state.breadcrumb.set(vec![
                                            "Database".to_string(),
                                            "Dashboard".to_string(),
                                        ]);
                                        state.write_enabled.set(false);
                                        state.last_result.set(Some(OperationResult::Success(
                                            "Database opened".into(),
                                        )));
                                    }
                                    Err(e) => {
                                        state.last_result.set(Some(OperationResult::Error(
                                            format!("Failed to open: {e}"),
                                        )));
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
                                match EngineHandle::open(&path).await {
                                    Ok(handle) => {
                                        let storage = Arc::clone(&handle.storage);
                                        let handle = Arc::new(handle);
                                        state.engine.set(Some(handle));
                                        state.db.set(Some(storage));
                                        state.nav.set(NavSection::Dashboard);
                                        state.breadcrumb.set(vec![
                                            "Database".to_string(),
                                            "Dashboard".to_string(),
                                        ]);
                                        state.write_enabled.set(true);
                                        state.last_result.set(Some(OperationResult::Success(
                                            "New database created".into(),
                                        )));
                                    }
                                    Err(e) => {
                                        state.last_result.set(Some(OperationResult::Error(
                                            format!("Failed to create: {e}"),
                                        )));
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
                            spawn(async move {
                                state.engine.set(None);
                                state.db.set(None);
                                state.write_enabled.set(false);
                                state.breadcrumb.set(vec!["Database".to_string()]);
                                state.last_result.set(Some(OperationResult::Success(
                                    "Database closed".into(),
                                )));
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
                                            state.last_result.set(Some(OperationResult::Success(
                                                "Checkpoint complete".into(),
                                            )));
                                        }
                                        Err(e) => {
                                            state.last_result.set(Some(OperationResult::Error(
                                                format!("Checkpoint failed: {e}"),
                                            )));
                                        }
                                    }
                                }
                            });
                        },
                        "Checkpoint"
                    }
                    span { class: "toolbar-info", "Dirty: {dirty}" }
                }

                div { class: "toolbar-separator" }

                {
                    let badge_class = if write_enabled {
                        "rw-badge unlocked"
                    } else {
                        "rw-badge locked"
                    };
                    let badge_text = if write_enabled {
                        "READ-WRITE"
                    } else {
                        "READ-ONLY"
                    };
                    rsx! {
                        div {
                            class: "{badge_class}",
                            onclick: move |_| {
                                state.write_enabled.set(!write_enabled);
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
    let handle = rfd::AsyncFileDialog::new()
        .set_title("Choose Directory for New Database")
        .pick_folder()
        .await?;
    let path = handle.path().to_path_buf();

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
