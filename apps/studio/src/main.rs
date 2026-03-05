mod app;
mod components;
mod engine;
mod modules;
mod state;
mod theme;

fn main() {
    dioxus::launch(app::App);
}
