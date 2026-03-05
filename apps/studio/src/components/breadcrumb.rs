use dioxus::prelude::*;

use crate::state::AppState;

#[component]
pub fn Breadcrumb() -> Element {
    let state = use_context::<AppState>();
    let crumbs = state.breadcrumb.read().clone();

    rsx! {
        div { class: "breadcrumb",
            for (i, crumb) in crumbs.iter().enumerate() {
                if i > 0 {
                    span { class: "breadcrumb-separator", ">" }
                }
                span {
                    class: if i < crumbs.len() - 1 { "breadcrumb-link" } else { "" },
                    "{crumb}"
                }
            }
        }
    }
}
