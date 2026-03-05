use dioxus::prelude::*;

/// Hex dump display component. Renders bytes in `OFFSET: HH HH ... | ASCII` format.
#[component]
pub fn HexDump(
    data: Vec<u8>,
    #[props(default = 16)] bytes_per_line: usize,
) -> Element {
    let lines: Vec<String> = data
        .chunks(bytes_per_line)
        .enumerate()
        .map(|(i, chunk)| {
            let offset = i * bytes_per_line;
            let hex: String = chunk
                .iter()
                .map(|b| format!("{b:02X}"))
                .collect::<Vec<_>>()
                .join(" ");
            let ascii: String = chunk
                .iter()
                .map(|&b| if b.is_ascii_graphic() || b == b' ' { b as char } else { '.' })
                .collect();
            let padding = " ".repeat((bytes_per_line - chunk.len()) * 3);
            format!("{offset:08X}  {hex}{padding}  |{ascii}|")
        })
        .collect();

    rsx! {
        div { class: "hex-dump",
            for line in &lines {
                "{line}\n"
            }
        }
    }
}
