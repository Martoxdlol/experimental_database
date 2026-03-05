use dioxus::prelude::*;

use crate::state::{AppState, OperationResult};

#[component]
pub fn Toast() -> Element {
    let mut state = use_context::<AppState>();
    let result = state.last_result.read().clone();

    if let Some(result) = result {
        // Auto-dismiss after 3 seconds
        spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            state.last_result.set(None);
        });

        let (class, text) = match &result {
            OperationResult::Success(msg) => ("toast success", msg.clone()),
            OperationResult::Error(msg) => ("toast error", msg.clone()),
        };

        rsx! {
            div {
                class: "{class}",
                onclick: move |_| state.last_result.set(None),
                "{text}"
            }
        }
    } else {
        rsx! {}
    }
}
