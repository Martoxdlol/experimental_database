use dioxus::prelude::*;

use crate::state::{AppState, Module};

#[component]
pub fn Sidebar() -> Element {
    let mut state = use_context::<AppState>();
    let active = *state.active_module.read();
    let db_open = state.is_open();

    rsx! {
        div { class: "sidebar",
            for module in Module::ALL {
                {
                    let m = *module;
                    let is_active = m == active;
                    let disabled = !db_open;
                    let class = if disabled {
                        "sidebar-item disabled"
                    } else if is_active {
                        "sidebar-item active"
                    } else {
                        "sidebar-item"
                    };
                    rsx! {
                        div {
                            class: "{class}",
                            onclick: move |_| {
                                if db_open {
                                    state.active_module.set(m);
                                    state.breadcrumb.set(vec!["Database".to_string(), m.label().to_string()]);
                                }
                            },
                            div { class: "sidebar-icon", "{m.icon()}" }
                            "{m.label()}"
                        }
                    }
                }
            }
        }
    }
}
