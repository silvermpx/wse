// The plugin's commands, for the permissions tauri-plugin writes (each
// command's allow and deny) and the application's build reads.
const COMMANDS: &[&str] = &["attach", "frames", "detach"];

fn main() {
    tauri_plugin::Builder::new(COMMANDS).build();
}
