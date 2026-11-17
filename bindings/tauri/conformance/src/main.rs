//! The conformance cases in a real Tauri webview: the owner of the
//! connection on this Rust side (tauri-plugin-wse), and a hidden window
//! whose page runs the cases with its own client (wse-client/tauri). The
//! page asks for an owner as each case wants it, and says how the cases
//! went; the application ends with that.
//!
//!     node conformance/build.mjs && cargo build -p wse-tauri-conformance
//!     target/debug/wse-tauri-conformance ws://127.0.0.1:P/control [case,...]

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tauri::utils::config::BackgroundThrottlingPolicy;
use tauri::{AppHandle, State, WebviewUrl, WebviewWindowBuilder};
use tauri_plugin_wse::WseExt;
use wse_client::{Config, Encryption, Format};

/// What the page is told: where the conformance server is, and what runs it.
#[derive(Clone, Serialize)]
struct Given {
    control: String,
    cases: Option<String>,
    os: &'static str,
    arch: &'static str,
    runtime: String,
}

/// A case's connection, as the page asks for it.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Asked {
    url: String,
    token: String,
    format: String,
    e2e_key: Option<String>,
    signing: bool,
    server_signing_key: Option<String>,
}

#[tauri::command]
fn harness_given(given: State<'_, Given>) -> Given {
    given.inner().clone()
}

/// Starts the owner as the case asks: the last case's closes.
#[tauri::command]
async fn harness_owner(app: AppHandle, asked: Asked) -> Result<Value, String> {
    let mut config = Config::new(&asked.url);
    config.token = Some(asked.token);
    config.reconnect.base_delay = Duration::from_millis(50);
    config.reconnect.max_delay = Duration::from_millis(300);
    config.reconnect.jitter = false;
    if asked.format == "msgpack" {
        config.format = Format::Msgpack;
    }
    if let Some(key) = &asked.e2e_key {
        config.encryption = Some(Encryption::pinned([key.as_str()]));
    }
    let mut public = None;
    if asked.signing {
        let key = wse_client::generate_key();
        public = Some(wse_client::public_key(&key).map_err(|e| e.to_string())?);
        config.signing_key = Some(key);
        config.server_signing_keys = asked.server_signing_key.into_iter().collect();
    }
    app.wse().start(config).await.map_err(|e| e.to_string())?;
    Ok(json!({ "publicKey": public }))
}

#[tauri::command]
fn harness_done(app: AppHandle, failed: Vec<String>) {
    for failure in &failed {
        eprintln!("failed: {failure}");
    }
    if failed.is_empty() {
        println!("tauri: every case passed");
    } else {
        println!("tauri: {} case(s) failed", failed.len());
    }
    app.exit(i32::from(!failed.is_empty()));
}

fn main() {
    let mut args = std::env::args().skip(1);
    let Some(control) = args.next() else {
        eprintln!("usage: wse-tauri-conformance ws://127.0.0.1:P/control [case,...]");
        std::process::exit(2);
    };
    let given = Given {
        control,
        cases: args.next(),
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        runtime: format!("tauri {}", tauri::VERSION),
    };
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(600));
        eprintln!("tauri: the cases did not end within 10 minutes");
        std::process::exit(2);
    });
    tauri::Builder::default()
        .plugin(tauri_plugin_wse::init())
        .manage(given)
        .invoke_handler(tauri::generate_handler![
            harness_given,
            harness_owner,
            harness_done
        ])
        .setup(|app| {
            WebviewWindowBuilder::new(app, "main", WebviewUrl::App("index.html".into()))
                .visible(false)
                .background_throttling(BackgroundThrottlingPolicy::Disabled)
                .build()?;
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("the Tauri application");
}
