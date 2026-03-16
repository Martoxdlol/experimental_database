use dioxus::prelude::*;

use crate::state::{AppState, InternalsTool, NavSection};

#[component]
pub fn NavSidebar() -> Element {
    let mut state = use_context::<AppState>();
    let nav = *state.nav.read();
    let db_open = state.is_open();
    let internals_expanded = *state.internals_expanded.read();

    rsx! {
        div { class: "nav-sidebar",
            // ─── Database Section ───
            div { class: "nav-section-header", "DATABASE" }

            NavItem {
                label: "Dashboard",
                active: matches!(nav, NavSection::Dashboard),
                disabled: !db_open,
                onclick: move |_| {
                    state.nav.set(NavSection::Dashboard);
                    state.breadcrumb.set(vec!["Database".to_string(), "Dashboard".to_string()]);
                },
            }
            NavItem {
                label: "Collections",
                active: matches!(nav, NavSection::Collections),
                disabled: !db_open,
                onclick: move |_| {
                    state.nav.set(NavSection::Collections);
                    state.selected_collection.set(None);
                    state.breadcrumb.set(vec!["Database".to_string(), "Collections".to_string()]);
                },
            }
            NavItem {
                label: "Query Workbench",
                active: matches!(nav, NavSection::QueryWorkbench),
                disabled: !db_open,
                onclick: move |_| {
                    state.nav.set(NavSection::QueryWorkbench);
                    state.breadcrumb.set(vec!["Database".to_string(), "Query Workbench".to_string()]);
                },
            }
            NavItem {
                label: "Subscriptions",
                active: matches!(nav, NavSection::Subscriptions),
                disabled: !db_open,
                onclick: move |_| {
                    state.nav.set(NavSection::Subscriptions);
                    state.breadcrumb.set(vec!["Database".to_string(), "Subscriptions".to_string()]);
                },
            }
            NavItem {
                label: "Configuration",
                active: matches!(nav, NavSection::Config),
                disabled: !db_open,
                onclick: move |_| {
                    state.nav.set(NavSection::Config);
                    state.breadcrumb.set(vec!["Database".to_string(), "Configuration".to_string()]);
                },
            }

            // ─── Internals Section ───
            div {
                class: "nav-section-header clickable",
                onclick: move |_| {
                    state.internals_expanded.set(!internals_expanded);
                },
                if internals_expanded { "\u{25BE} INTERNALS" } else { "\u{25B8} INTERNALS" }
            }

            if internals_expanded {
                // Storage
                div { class: "nav-group-header", "Storage" }
                for tool in InternalsTool::STORAGE {
                    {
                        let t = *tool;
                        let active = nav == NavSection::Internals(t);
                        rsx! {
                            NavItem {
                                label: t.label(),
                                active: active,
                                nested: true,
                                disabled: !db_open,
                                onclick: move |_| {
                                    state.nav.set(NavSection::Internals(t));
                                    state.breadcrumb.set(vec![
                                        "Database".to_string(),
                                        "Internals".to_string(),
                                        t.group().to_string(),
                                        t.label().to_string(),
                                    ]);
                                },
                            }
                        }
                    }
                }

                // Docstore
                div { class: "nav-group-header", "Docstore" }
                for tool in InternalsTool::DOCSTORE {
                    {
                        let t = *tool;
                        let active = nav == NavSection::Internals(t);
                        rsx! {
                            NavItem {
                                label: t.label(),
                                active: active,
                                nested: true,
                                disabled: !db_open,
                                onclick: move |_| {
                                    state.nav.set(NavSection::Internals(t));
                                    state.breadcrumb.set(vec![
                                        "Database".to_string(),
                                        "Internals".to_string(),
                                        t.group().to_string(),
                                        t.label().to_string(),
                                    ]);
                                },
                            }
                        }
                    }
                }

                // Query Engine
                div { class: "nav-group-header", "Query Engine" }
                for tool in InternalsTool::QUERY {
                    {
                        let t = *tool;
                        let active = nav == NavSection::Internals(t);
                        rsx! {
                            NavItem {
                                label: t.label(),
                                active: active,
                                nested: true,
                                disabled: !db_open,
                                onclick: move |_| {
                                    state.nav.set(NavSection::Internals(t));
                                    state.breadcrumb.set(vec![
                                        "Database".to_string(),
                                        "Internals".to_string(),
                                        t.group().to_string(),
                                        t.label().to_string(),
                                    ]);
                                },
                            }
                        }
                    }
                }

                // Console
                {
                    let t = InternalsTool::Console;
                    let active = nav == NavSection::Internals(t);
                    rsx! {
                        NavItem {
                            label: "Console",
                            active: active,
                            disabled: !db_open,
                            onclick: move |_| {
                                state.nav.set(NavSection::Internals(t));
                                state.breadcrumb.set(vec![
                                    "Database".to_string(),
                                    "Internals".to_string(),
                                    "Console".to_string(),
                                ]);
                            },
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn NavItem(
    label: &'static str,
    active: bool,
    #[props(default = false)] nested: bool,
    #[props(default = false)] disabled: bool,
    onclick: EventHandler<MouseEvent>,
) -> Element {
    let class = if disabled {
        if nested { "nav-item nested disabled" } else { "nav-item disabled" }
    } else if active {
        if nested { "nav-item nested active" } else { "nav-item active" }
    } else if nested {
        "nav-item nested"
    } else {
        "nav-item"
    };

    rsx! {
        div {
            class: "{class}",
            onclick: move |evt| {
                if !disabled {
                    onclick.call(evt);
                }
            },
            "{label}"
        }
    }
}
