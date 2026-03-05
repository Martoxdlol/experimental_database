use dioxus::desktop::{Config, WindowBuilder};
use dioxus::prelude::*;
use dioxus::desktop::tao::window::Icon;

mod app;
mod components;
mod engine;
mod modules;
mod state;
mod theme;

fn main() {
    let mut cfg = Config::new().with_window(
        WindowBuilder::new().with_title("exdb studio"),
    );
    if let Ok(icon) = make_icon() {
        cfg = cfg.with_icon(icon);
    }
    cfg = cfg.with_custom_head(r#"<script>
new MutationObserver(()=>{
document.querySelectorAll('input,textarea').forEach(el=>{
el.setAttribute('autocorrect','off');
el.setAttribute('autocapitalize','off');
el.setAttribute('autocomplete','off');
el.setAttribute('spellcheck','false');
});
}).observe(document.body||document.documentElement,{childList:true,subtree:true});
</script>"#.into());
    LaunchBuilder::desktop()
        .with_cfg(cfg)
        .launch(app::App);
}

/// Generate a 32x32 RGBA app icon: a blue database cylinder.
fn make_icon() -> Result<Icon, dioxus::desktop::tao::window::BadIcon> {
    const S: u32 = 32;
    let mut rgba = vec![0u8; (S * S * 4) as usize];
    let cx = S as f64 / 2.0;
    let cy_top = 9.0;
    let cy_bot = 23.0;
    let rx = 12.0;
    let ry_top = 5.0;
    let ry_bot = 5.0;
    let col: [u8; 3] = [0x7a, 0xa2, 0xf7]; // accent blue

    for y in 0..S {
        for x in 0..S {
            let fx = x as f64 + 0.5;
            let fy = y as f64 + 0.5;
            let dx = (fx - cx) / rx;

            // Is this pixel inside the cylinder shape?
            let in_top_ellipse = dx * dx + ((fy - cy_top) / ry_top).powi(2) <= 1.0;
            let in_bot_ellipse = dx * dx + ((fy - cy_bot) / ry_bot).powi(2) <= 1.0;
            let in_body = dx.abs() <= 1.0 && fy >= cy_top && fy <= cy_bot;

            let alpha = if in_top_ellipse {
                255 // bright top cap
            } else if in_body {
                180 // body
            } else if in_bot_ellipse {
                140 // bottom cap
            } else {
                0
            };

            if alpha > 0 {
                let i = ((y * S + x) * 4) as usize;
                rgba[i] = col[0];
                rgba[i + 1] = col[1];
                rgba[i + 2] = col[2];
                rgba[i + 3] = alpha;
            }
        }
    }
    Icon::from_rgba(rgba, S, S)
}
