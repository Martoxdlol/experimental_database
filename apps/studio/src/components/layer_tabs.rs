use dioxus::prelude::*;

use crate::state::{AppState, LayerTab};

#[component]
pub fn LayerTabBar() -> Element {
    let mut state = use_context::<AppState>();
    let active = *state.active_tab.read();
    let db_open = state.is_open();

    rsx! {
        div { class: "layer-tabs",
            for tab in LayerTab::ALL {
                {
                    let t = *tab;
                    let is_active = t == active;
                    let disabled = !db_open && t != LayerTab::Overview;
                    let class = if disabled {
                        "layer-tab disabled"
                    } else if is_active {
                        "layer-tab active"
                    } else {
                        "layer-tab"
                    };
                    rsx! {
                        div {
                            class: "{class}",
                            onclick: move |_| {
                                if db_open || t == LayerTab::Overview {
                                    state.active_tab.set(t);
                                    state.breadcrumb.set(vec![
                                        "Database".to_string(),
                                        t.label().to_string(),
                                    ]);
                                }
                            },
                            "{t.label()}"
                        }
                    }
                }
            }
        }
    }
}
