//! A Tauri application with tauri-plugin-wse and the packed wse-client: the
//! connection on this Rust side, two windows rendered with wse-client/react.
//! It checks itself against the server WSE_URL names: both windows connected
//! and taking ticks on one connection; then one closes, and the other goes
//! on. Exit 0 when it all holds.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use tauri::{AppHandle, Manager, State, WebviewUrl, WebviewWindowBuilder, Window};
use tauri_plugin_wse::{WseExt, wse_client};

/// What each window last said it saw: its connection, and the ticks it took.
#[derive(Default)]
struct Seen(Mutex<HashMap<String, (String, u64)>>);

impl Seen {
    fn of(&self, label: &str) -> (String, u64) {
        let seen = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        seen.get(label).cloned().unwrap_or_default()
    }
}

#[tauri::command]
fn check_seen(window: Window, seen: State<'_, Seen>, connection: String, ticks: u64) {
    let mut all = seen.0.lock().unwrap_or_else(PoisonError::into_inner);
    all.insert(window.label().to_owned(), (connection, ticks));
}

fn until(what: &str, within: Duration, test: impl Fn() -> bool) -> Result<(), String> {
    let end = Instant::now() + within;
    while Instant::now() < end {
        if test() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(format!("{what}: not within {within:?}"))
}

fn check(app: &AppHandle) -> Result<(), String> {
    let seen = app.state::<Seen>();
    let attached = || app.wse().owner().map_or(0, |owner| owner.attached());
    until(
        "both windows connected, taking ticks",
        Duration::from_secs(30),
        || {
            ["window-1", "window-2"].iter().all(|label| {
                let (connection, ticks) = seen.of(label);
                connection == "connected" && ticks >= 3
            })
        },
    )?;
    if attached() != 2 {
        return Err(format!("{} windows attached, not 2", attached()));
    }
    // One window closes: the other keeps the connection, and its ticks.
    app.get_webview_window("window-1")
        .ok_or("no window-1")?
        .destroy()
        .map_err(|e| e.to_string())?;
    let (_, before) = seen.of("window-2");
    until("the other window going on", Duration::from_secs(10), || {
        let (connection, ticks) = seen.of("window-2");
        connection == "connected" && ticks >= before + 5
    })?;
    until("the closed window let go", Duration::from_secs(5), || {
        attached() == 1
    })
}

fn main() {
    let url = std::env::var("WSE_URL").expect("WSE_URL names the server");
    tauri::Builder::default()
        .plugin(tauri_plugin_wse::init())
        .manage(Seen::default())
        .invoke_handler(tauri::generate_handler![check_seen])
        .setup(move |app| {
            // What it keeps goes with the run.
            let dir = std::env::temp_dir().join(format!("wse-tauri-check-{}", std::process::id()));
            std::fs::create_dir_all(&dir)?;
            let mut config = wse_client::Config::new(&url);
            config.offline = Some(wse_client::Offline::in_file(dir.join("wse-outbox")));
            tauri::async_runtime::block_on(app.wse().start(config))?;
            for n in 1..=2 {
                WebviewWindowBuilder::new(
                    app,
                    format!("window-{n}"),
                    WebviewUrl::App("index.html".into()),
                )
                .title("WSE")
                .visible(false)
                .initialization_script(format!("window.__WSE_CHECK__ = {{ window: {n} }};"))
                .build()?;
            }
            let app = app.handle().clone();
            std::thread::spawn(move || match check(&app) {
                Ok(()) => {
                    println!(
                        "tauri check: two windows on one connection, one closed, the other went on"
                    );
                    app.exit(0);
                }
                Err(why) => {
                    eprintln!("tauri check: {why}");
                    app.exit(1);
                }
            });
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("the check's application");
}
