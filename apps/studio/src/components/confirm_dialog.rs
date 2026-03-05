use dioxus::prelude::*;

#[component]
pub fn ConfirmDialog(
    title: String,
    message: String,
    on_confirm: EventHandler<()>,
    on_cancel: EventHandler<()>,
    danger: Option<bool>,
) -> Element {
    let btn_class = if danger.unwrap_or(false) {
        "btn btn-danger"
    } else {
        "btn btn-action"
    };

    rsx! {
        div { class: "dialog-overlay",
            onclick: move |_| on_cancel.call(()),
            div {
                class: "dialog",
                onclick: move |e| e.stop_propagation(),
                div { class: "dialog-title", "{title}" }
                div { class: "dialog-body", "{message}" }
                div { class: "dialog-actions",
                    button {
                        class: "btn",
                        onclick: move |_| on_cancel.call(()),
                        "Cancel"
                    }
                    button {
                        class: "{btn_class}",
                        onclick: move |_| on_confirm.call(()),
                        "Confirm"
                    }
                }
            }
        }
    }
}
