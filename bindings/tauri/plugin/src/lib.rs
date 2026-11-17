//! One WSE connection for every window of a Tauri application.
//!
//! The Rust side holds the connection (a [`wse_client::bridge::Owner`]),
//! with the token, the keys and the outbox; each window's page holds a
//! client of the npm package's `wse-client/tauri` and talks to it through
//! this plugin's commands. Register the plugin, start the connection once
//! the user is known, and give the windows the plugin's permission
//! (`wse:default`) in their capabilities:
//!
//! ```ignore
//! use tauri_plugin_wse::WseExt;
//!
//! tauri::Builder::default()
//!     .plugin(tauri_plugin_wse::init())
//!     .setup(|app| {
//!         let mut config = wse_client::Config::new("wss://example.com/wse");
//!         config.token = Some("...".to_owned());
//!         let app = app.handle().clone();
//!         tauri::async_runtime::spawn(async move { app.wse().start(config).await });
//!         Ok(())
//!     })
//!     .run(tauri::generate_context!())
//!     .expect("the application");
//! ```
//!
//! ```js
//! import { invoke, Channel } from '@tauri-apps/api/core';
//! import { createTauriClient } from 'wse-client/tauri';
//!
//! const client = createTauriClient({ invoke, Channel });
//! ```
//!
//! A page that reloads, or a window that closes, lets its share of the
//! connection go. Starting the connection again (another user) lets the
//! windows of the last one go; [`Wse::sign_out`] lets every window go and
//! forgets what the connection kept for the user.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use serde_json::value::RawValue;
use tauri::ipc::{Channel, InvokeResponseBody};
use tauri::plugin::{Builder, TauriPlugin};
use tauri::webview::PageLoadEvent;
use tauri::{Manager, RunEvent, Runtime, State, Webview, WindowEvent};

pub use wse_client;
use wse_client::bridge::{Options, Owner, Window};
use wse_client::{Config, Error};

/// The application's WSE connection, and the pages attached to it: the
/// plugin's state, [`WseExt::wse`] from anything of the application.
#[derive(Default)]
pub struct Wse {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    owner: Option<Owner>,
    /// Each page attached, by the number its client was given.
    attached: HashMap<u64, Attached>,
    next: u64,
}

/// A page attached: its webview, the owner, and its window of the owner.
struct Attached {
    label: String,
    owner: Owner,
    window: u64,
}

/// A page's channel: the owner's frames go to its client.
struct Page(Channel<InvokeResponseBody>);

impl Window for Page {
    fn post(&self, frame: String) {
        // A page that went takes no more; the plugin lets it go.
        let _ = self.0.send(InvokeResponseBody::Json(frame));
    }

    fn close(&self) {}
}

impl Wse {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts the application's connection, for the windows' pages to
    /// attach to; it connects while a page (or the application,
    /// [`Owner::retain`]) shares it. A connection started before (the last
    /// user's) closes, and its pages are let go.
    pub async fn start(&self, config: Config) -> Result<Owner, Error> {
        self.start_with(config, Options::default()).await
    }

    /// [`start`](Self::start) with the bridge's options.
    pub async fn start_with(&self, config: Config, options: Options) -> Result<Owner, Error> {
        let owner = Owner::start(config, options).await?;
        let last = {
            let mut inner = self.lock();
            inner.attached.clear();
            inner.owner.replace(owner.clone())
        };
        if let Some(last) = last {
            last.close().await;
        }
        Ok(owner)
    }

    /// The connection started last, if any.
    pub fn owner(&self) -> Option<Owner> {
        self.lock().owner.clone()
    }

    /// The user signs out: every page is let go, and the connection with
    /// what it kept for the user.
    pub fn sign_out(&self) {
        let owner = {
            let mut inner = self.lock();
            inner.attached.clear();
            inner.owner.take()
        };
        if let Some(owner) = owner {
            owner.sign_out();
        }
    }

    /// The connection ends: every page is let go; what it keeps in a file
    /// stays, for the next start.
    pub async fn close(&self) {
        let owner = {
            let mut inner = self.lock();
            inner.attached.clear();
            inner.owner.take()
        };
        if let Some(owner) = owner {
            owner.close().await;
        }
    }

    fn attach(&self, label: &str, channel: Channel<InvokeResponseBody>) -> Result<u64, String> {
        let mut inner = self.lock();
        let owner = inner.owner.clone().ok_or_else(|| {
            "the application started no WSE connection (app.wse().start(config))".to_owned()
        })?;
        let window = owner.attach(Page(channel));
        inner.next += 1;
        let id = inner.next;
        inner.attached.insert(
            id,
            Attached {
                label: label.to_owned(),
                owner,
                window,
            },
        );
        Ok(id)
    }

    fn frames(&self, id: u64, frames: &[Box<RawValue>]) -> Result<(), String> {
        let (owner, window) = {
            let inner = self.lock();
            let attached = inner
                .attached
                .get(&id)
                .ok_or_else(|| "this page's window was let go".to_owned())?;
            (attached.owner.clone(), attached.window)
        };
        for frame in frames {
            owner.frame(window, frame.get());
        }
        Ok(())
    }

    fn detach(&self, id: u64) {
        let attached = self.lock().attached.remove(&id);
        if let Some(attached) = attached {
            attached.owner.detach(attached.window);
        }
    }

    /// The pages of webview `label` went (a reload, a window closed).
    fn let_go(&self, label: &str) {
        let gone: Vec<Attached> = {
            let mut inner = self.lock();
            let ids: Vec<u64> = inner
                .attached
                .iter()
                .filter(|(_, attached)| attached.label == label)
                .map(|(id, _)| *id)
                .collect();
            ids.iter()
                .filter_map(|id| inner.attached.remove(id))
                .collect()
        };
        for attached in gone {
            attached.owner.detach(attached.window);
        }
    }
}

/// The application's WSE connection, from the app, a window or a webview.
pub trait WseExt<R: Runtime> {
    fn wse(&self) -> &Wse;
}

impl<R: Runtime, T: Manager<R>> WseExt<R> for T {
    fn wse(&self) -> &Wse {
        self.state::<Wse>().inner()
    }
}

/// A page attaches: the owner's frames come on `on_frame`.
#[tauri::command]
fn attach<R: Runtime>(
    webview: Webview<R>,
    wse: State<'_, Wse>,
    on_frame: Channel<InvokeResponseBody>,
) -> Result<u64, String> {
    wse.attach(webview.label(), on_frame)
}

/// The frames a page sent, in order, for its window.
#[tauri::command]
fn frames(wse: State<'_, Wse>, att: u64, frames: Vec<Box<RawValue>>) -> Result<(), String> {
    wse.frames(att, &frames)
}

/// A page lets its window go.
#[tauri::command]
fn detach(wse: State<'_, Wse>, att: u64) {
    wse.detach(att);
}

/// The plugin: register it with `tauri::Builder::plugin`.
pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::new("wse")
        .invoke_handler(tauri::generate_handler![attach, frames, detach])
        .setup(|app, _api| {
            app.manage(Wse::default());
            Ok(())
        })
        .on_page_load(|webview, payload| {
            // The page before is gone: its share goes with it.
            if payload.event() == PageLoadEvent::Started {
                webview.wse().let_go(webview.label());
            }
        })
        .on_event(|app, event| {
            if let RunEvent::WindowEvent {
                label,
                event: WindowEvent::Destroyed,
                ..
            } = event
            {
                app.wse().let_go(label);
            }
        })
        .build()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::{Value, json};
    use tokio::sync::mpsc;

    use super::*;

    const SECRET: &str = "a-test-secret-of-at-least-thirty-two-bytes";

    struct App;

    impl wse::Handler for App {
        type Connection = ();

        fn connect(&self, _connect: &wse::Connect<'_>) -> Option<()> {
            Some(())
        }

        fn message(&self, _conn: &mut (), _message: wse::Inbound, _size: usize) -> bool {
            true
        }

        fn signed(&self, _conn: &mut (), _message: Value, _size: usize) -> bool {
            true
        }

        fn disconnect(&self, _conn: ()) {}
    }

    /// A WSE server, and a config of a user's for it.
    async fn server() -> (wse::Server, Config) {
        let mut settings = wse::Config::default();
        settings.jwt = Some(wse::JwtSettings::new(SECRET));
        settings.client_subscriptions = true;
        let server = wse::Server::new(settings).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = Config::new(format!("ws://{}/wse", listener.local_addr().unwrap()));
        server.start(listener, App).unwrap();
        config.token = Some(
            server
                .publisher()
                .issue_token("ann", Duration::from_secs(600), serde_json::Map::new())
                .unwrap(),
        );
        (server, config)
    }

    /// A page's channel, as the frontend's would be: what the owner sends it.
    fn channel() -> (Channel<InvokeResponseBody>, mpsc::UnboundedReceiver<Value>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let channel = Channel::new(move |body| {
            if let InvokeResponseBody::Json(json) = body {
                let _ = tx.send(serde_json::from_str(&json).expect("a frame is JSON"));
            }
            Ok(())
        });
        (channel, rx)
    }

    fn frames(values: &[Value]) -> Vec<Box<RawValue>> {
        values
            .iter()
            .map(|value| RawValue::from_string(value.to_string()).unwrap())
            .collect()
    }

    /// Takes what the page gets until `test` holds, acknowledging each batch.
    async fn until(
        wse: &Wse,
        id: u64,
        rx: &mut mpsc::UnboundedReceiver<Value>,
        test: impl Fn(&Value) -> bool,
    ) -> Value {
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("a frame within 5 s")
                .expect("the channel lives");
            if frame["k"] == "b" {
                let _ = wse.frames(id, &frames(&[json!({"k": "ack", "s": frame["s"]})]));
                if let Some(item) = frame["items"]
                    .as_array()
                    .and_then(|items| items.iter().find(|item| test(item)))
                {
                    return item.clone();
                }
            } else if test(&frame) {
                return frame;
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_page_attaches_once_the_application_started_the_connection() {
        let (server, config) = server().await;
        let wse = Wse::default();
        let (early, _) = channel();
        let refused = wse.attach("main", early).unwrap_err();
        assert!(refused.contains("app.wse().start"), "{refused}");
        let owner = wse.start(config).await.unwrap();
        let (page, mut rx) = channel();
        let id = wse.attach("main", page).unwrap();
        wse.frames(
            id,
            &frames(&[
                json!({"k": "hello", "v": 1, "agent": "test"}),
                json!({"k": "share", "on": true}),
            ]),
        )
        .unwrap();
        until(&wse, id, &mut rx, |f| f["k"] == "welcome").await;
        let connected = until(&wse, id, &mut rx, |i| i["ev"] == "connected").await;
        assert_eq!(connected["user"], "ann");
        assert!(owner.client().is_connected());
        wse.close().await;
        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_pages_of_a_webview_that_reloads_are_let_go() {
        let (server, config) = server().await;
        let wse = Wse::default();
        let owner = wse.start(config).await.unwrap();
        let (main, _main_rx) = channel();
        let (other, _other_rx) = channel();
        let gone = wse.attach("main", main).unwrap();
        let kept = wse.attach("settings", other).unwrap();
        assert_eq!(owner.attached(), 2);
        wse.let_go("main");
        assert_eq!(owner.attached(), 1);
        assert!(wse.frames(gone, &frames(&[json!({"k": "bye"})])).is_err());
        assert!(wse.frames(kept, &frames(&[json!({"k": "bye"})])).is_ok());
        assert_eq!(owner.attached(), 0);
        wse.close().await;
        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_connection_started_again_lets_the_last_ones_pages_go() {
        let (server, config) = server().await;
        let wse = Wse::default();
        wse.start(config.clone()).await.unwrap();
        let (page, mut rx) = channel();
        let id = wse.attach("main", page).unwrap();
        wse.frames(
            id,
            &frames(&[json!({"k": "hello", "v": 1, "agent": "test"})]),
        )
        .unwrap();
        until(&wse, id, &mut rx, |f| f["k"] == "welcome").await;
        // Another user's connection: the last one's pages end with it.
        let next = wse.start(config).await.unwrap();
        let revoked = until(&wse, id, &mut rx, |f| f["k"] == "revoked").await;
        assert_eq!(revoked["reason"], "owner_closed");
        assert!(wse.frames(id, &frames(&[json!({"k": "bye"})])).is_err());
        assert_eq!(next.attached(), 0);
        wse.close().await;
        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_user_who_signs_out_takes_every_page_with_them() {
        let (server, config) = server().await;
        let wse = Wse::default();
        wse.start(config).await.unwrap();
        let (page, mut rx) = channel();
        let id = wse.attach("main", page).unwrap();
        wse.frames(
            id,
            &frames(&[json!({"k": "hello", "v": 1, "agent": "test"})]),
        )
        .unwrap();
        until(&wse, id, &mut rx, |f| f["k"] == "welcome").await;
        wse.sign_out();
        let revoked = until(&wse, id, &mut rx, |f| f["k"] == "revoked").await;
        assert_eq!(revoked["reason"], "signed_out");
        assert!(wse.owner().is_none());
        let (late, _) = channel();
        assert!(wse.attach("main", late).is_err());
        server.stop().await;
    }
}
