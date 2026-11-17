//! The example application of Tauri's plugin: two windows on one
//! connection, held in this Rust code with the outbox in a file. Against the
//! example server:
//!
//!     cargo run -p wse-client --example server
//!     node bindings/tauri/example/build.mjs
//!     cargo run -p wse-tauri-example    (in bindings/tauri)
//!
//! WSE_URL names another server. With WSE_EXAMPLE_CHECK=1 it checks itself:
//! each window connects, takes ticks and has an echo delivered, then it
//! ends (exit 0, or 1 after 30 s).

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::time::Duration;

use tauri::{AppHandle, Manager, State, WebviewUrl, WebviewWindowBuilder};
use tauri_plugin_wse::WseExt;

const WINDOWS: u32 = 2;

/// The windows that work, when the application checks itself.
#[derive(Default)]
struct Working(Mutex<BTreeSet<u32>>);

#[tauri::command]
fn example_working(app: AppHandle, working: State<'_, Working>, window: u32) {
    let all = {
        let mut set = working
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set.insert(window);
        set.len() as u32 == WINDOWS
    };
    if all {
        println!("tauri example: every window works");
        app.exit(0);
    }
}

fn main() {
    let url = std::env::var("WSE_URL").unwrap_or_else(|_| "ws://127.0.0.1:5007/wse".to_owned());
    let check = std::env::var("WSE_EXAMPLE_CHECK").as_deref() == Ok("1");
    if check {
        std::thread::spawn(|| {
            std::thread::sleep(Duration::from_secs(30));
            eprintln!("tauri example: the windows did not all work within 30 s");
            std::process::exit(1);
        });
    }
    tauri::Builder::default()
        .plugin(tauri_plugin_wse::init())
        .manage(Working::default())
        .invoke_handler(tauri::generate_handler![example_working])
        .setup(move |app| {
            // A check keeps nothing past its run.
            let dir = if check {
                std::env::temp_dir().join(format!("wse-example-{}", std::process::id()))
            } else {
                app.path().app_data_dir()?
            };
            std::fs::create_dir_all(&dir)?;
            let mut config = wse_client::Config::new(&url);
            config.offline = Some(wse_client::Offline::in_file(dir.join("wse-outbox")));
            // Started before the windows, so that their pages find it.
            tauri::async_runtime::block_on(app.wse().start(config))?;
            for n in 1..=WINDOWS {
                WebviewWindowBuilder::new(
                    app,
                    format!("window-{n}"),
                    WebviewUrl::App("index.html".into()),
                )
                .title("WSE")
                .inner_size(460.0, 620.0)
                .position(60.0 + f64::from(n - 1) * 480.0, 60.0)
                .visible(!check)
                .initialization_script(format!(
                    "window.__WSE_EXAMPLE__ = {{ window: {n}, check: {check} }};"
                ))
                .build()?;
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("the example application");
}
