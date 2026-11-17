# tauri-plugin-wse

One [WSE](https://github.com/silvermpx/wse) connection for every window of a Tauri application.
The Rust side holds the connection, with the token, the keys and the outbox (in a file, so that
what is not answered yet outlives the application); each window's page holds a client of the npm
package's `wse-client/tauri` and talks to it through this plugin's commands. The windows share the
connection's topics: each is subscribed once, and each page gets the messages of the topics it
holds, in batches it acknowledges, so a page that falls behind holds back only itself.

## The Rust side

```toml
[dependencies]
tauri-plugin-wse = "3"
```

```rust,ignore
use tauri::Manager;
use tauri_plugin_wse::{WseExt, wse_client};

tauri::Builder::default()
    .plugin(tauri_plugin_wse::init())
    .setup(|app| {
        let mut config = wse_client::Config::new("wss://example.com/wse");
        config.token = Some(token); // the signed-in user's
        config.offline = Some(wse_client::Offline::in_file(
            app.path().app_data_dir()?.join("wse-outbox"),
        ));
        // It returns at once: the connection is made in the background.
        tauri::async_runtime::block_on(app.wse().start(config))?;
        Ok(())
    })
    .run(tauri::generate_context!())
    .expect("the application");
```

The connection is made while a page (or the application, `Owner::retain`) shares it. The plugin
re-exports `wse_client`, at the version it is built with; `app.wse().owner()` is the owner, whose
`client()` the Rust side sends on as on any client.

- `app.wse().start(config)` again, for another user, closes the last connection and lets its
  pages go.
- `app.wse().sign_out()` lets every page go and forgets what the connection kept for the user.
- `app.wse().close()` ends the connection; what it keeps in a file stays for the next start.

## Permissions

The windows that may use the connection get the plugin's default permission in their
capabilities (`src-tauri/capabilities/default.json`):

```json
{
  "identifier": "default",
  "windows": ["main"],
  "permissions": ["core:default", "wse:default"]
}
```

`wse:default` allows the plugin's three commands: `attach`, `frames` and `detach`.

## A page

```ts
import { invoke, Channel } from '@tauri-apps/api/core';
import { createTauriClient } from 'wse-client/tauri';

const client = createTauriClient({ invoke, Channel });
client.retain();
client.subscribe(['prices']);
client.onMessage('price_update', (message) => render(message.p));
```

The client has the `WSEClient` interface: the adapters for React, Vue, Svelte, Angular and Solid
take it as they take a client of their own. A page that reloads, or a window that closes, lets its
share of the connection go.

## An example

[bindings/tauri/example](https://github.com/silvermpx/wse/tree/main/bindings/tauri/example) is an
application with two windows on one connection; its `main.rs` says how to run it against the
example server.

## License

MIT
