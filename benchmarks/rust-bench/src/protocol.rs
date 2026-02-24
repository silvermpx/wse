use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Connect to WSE server and complete the handshake (server_ready + client_hello).
/// Returns the WebSocket stream ready for messaging.
pub async fn connect_and_handshake(
    host: &str,
    port: u16,
    token: &str,
    uri_params: &str,
    timeout_secs: u64,
) -> Result<WsStream, Box<dyn std::error::Error + Send + Sync>> {
    let uri = format!("ws://{}:{}/wse?{}", host, port, uri_params);
    let request = http::Request::builder()
        .uri(&uri)
        .header("Host", format!("{}:{}", host, port))
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", generate_key())
        .header("Cookie", format!("access_token={}", token))
        .body(())?;

    let (ws_stream, _response) = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        tokio_tungstenite::connect_async(request),
    )
    .await??;

    let mut ws = ws_stream;

    // Wait for server_ready
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        let msg = tokio::time::timeout_at(deadline, ws.next())
            .await?
            .ok_or("connection closed before server_ready")??;

        if let Some(parsed) = parse_wse_message(&msg) {
            if parsed.get("t").and_then(|t| t.as_str()) == Some("server_ready") {
                break;
            }
        }
    }

    // Send client_hello
    let hello = serde_json::json!({
        "t": "client_hello",
        "p": {
            "client_version": "wse-bench/0.1.0",
            "protocol_version": 1
        }
    });
    ws.send(Message::Text(hello.to_string().into())).await?;

    Ok(ws)
}

/// Connect with a specific source address (for >64K connections).
pub async fn connect_and_handshake_from(
    host: &str,
    port: u16,
    token: &str,
    uri_params: &str,
    source_addr: SocketAddr,
    timeout_secs: u64,
) -> Result<WsStream, Box<dyn std::error::Error + Send + Sync>> {
    let socket = tokio::net::TcpSocket::new_v4()?;
    socket.bind(source_addr)?;
    let target: SocketAddr = format!("{}:{}", host, port).parse()?;
    let tcp_stream = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        socket.connect(target),
    )
    .await??;

    let uri = format!("ws://{}:{}/wse?{}", host, port, uri_params);
    let request = http::Request::builder()
        .uri(&uri)
        .header("Host", format!("{}:{}", host, port))
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", generate_key())
        .header("Cookie", format!("access_token={}", token))
        .body(())?;

    let (ws_stream, _response) = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        tokio_tungstenite::client_async(request, MaybeTlsStream::Plain(tcp_stream)),
    )
    .await??;

    let mut ws = ws_stream;

    // Wait for server_ready
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        let msg = tokio::time::timeout_at(deadline, ws.next())
            .await?
            .ok_or("connection closed before server_ready")??;

        if let Some(parsed) = parse_wse_message(&msg) {
            if parsed.get("t").and_then(|t| t.as_str()) == Some("server_ready") {
                break;
            }
        }
    }

    // Send client_hello
    let hello = serde_json::json!({
        "t": "client_hello",
        "p": {
            "client_version": "wse-bench/0.1.0",
            "protocol_version": 1
        }
    });
    ws.send(Message::Text(hello.to_string().into())).await?;

    Ok(ws)
}

/// Parse a WSE message, handling all prefix formats.
pub fn parse_wse_message(msg: &Message) -> Option<serde_json::Value> {
    match msg {
        Message::Text(text) => parse_text_frame(text),
        Message::Binary(data) => parse_binary_frame(data),
        _ => None,
    }
}

/// Strip WSE/S/U prefix from text frame and parse JSON.
fn parse_text_frame(text: &str) -> Option<serde_json::Value> {
    let stripped = if text.starts_with("WSE{") || text.starts_with("WSE[") {
        &text[3..]
    } else if text.starts_with("S{")
        || text.starts_with("S[")
        || text.starts_with("U{")
        || text.starts_with("U[")
    {
        &text[1..]
    } else {
        text
    };
    serde_json::from_str(stripped).ok()
}

/// Handle binary frames: C: (zlib), M: (msgpack), E: (encrypted), raw zlib, plain JSON.
fn parse_binary_frame(data: &[u8]) -> Option<serde_json::Value> {
    if data.starts_with(b"C:") {
        // Zlib compressed
        decompress_and_parse(&data[2..])
    } else if data.starts_with(b"M:") {
        // Msgpack
        rmp_serde::from_slice(&data[2..]).ok()
    } else if data.starts_with(b"E:") {
        // Encrypted - cannot decode without session key
        None
    } else if !data.is_empty() && data[0] == 0x78 {
        // Raw zlib (magic byte)
        decompress_and_parse(data)
    } else {
        // Try as plain JSON in binary frame, with prefix stripping
        let text = std::str::from_utf8(data).ok()?;
        parse_text_frame(text)
    }
}

fn decompress_and_parse(data: &[u8]) -> Option<serde_json::Value> {
    use flate2::read::ZlibDecoder;
    use std::io::Read;
    let mut decoder = ZlibDecoder::new(data);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed).ok()?;
    parse_text_frame(&decompressed)
}

/// Build a WSE application-level PING message.
#[allow(dead_code)]
pub fn build_ping() -> String {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    serde_json::json!({
        "t": "ping",
        "p": { "client_timestamp": now_ms }
    })
    .to_string()
}

/// Check if a parsed message is a PONG (server sends uppercase "PONG").
pub fn is_pong(msg: &serde_json::Value) -> bool {
    msg.get("t")
        .and_then(|t| t.as_str())
        .map(|t| t.eq_ignore_ascii_case("pong"))
        .unwrap_or(false)
}

/// Build a generic message payload of approximately `size` bytes.
pub fn build_payload(size: usize) -> String {
    if size <= 64 {
        // Minimal trading payload
        serde_json::json!({
            "t": "trade_update",
            "p": {"s": "ES", "px": 5234.75, "q": 2}
        })
        .to_string()
    } else {
        // Pad with data field to reach target size
        let base = serde_json::json!({
            "t": "trade_update",
            "p": {
                "symbol": "ES",
                "side": "buy",
                "price": 5234.75,
                "qty": 2,
                "ts": "2026-02-23T14:30:00.123Z",
                "account": "acc_01HQ3",
                "tags": ["momentum", "breakout"],
                "score": 0.87
            }
        });
        let base_str = base.to_string();
        if base_str.len() + 20 >= size {
            return base_str;
        }
        // Add padding
        let padding_needed = size - base_str.len() - 20; // account for "data":"..."
        let padding: String = "x".repeat(padding_needed);
        let padded = serde_json::json!({
            "t": "trade_update",
            "p": {
                "symbol": "ES",
                "side": "buy",
                "price": 5234.75,
                "qty": 2,
                "ts": "2026-02-23T14:30:00.123Z",
                "account": "acc_01HQ3",
                "tags": ["momentum", "breakout"],
                "score": 0.87,
                "data": padding
            }
        });
        padded.to_string()
    }
}

/// Max ports per source IP (with some headroom for the server's own port).
const PORTS_PER_IP: usize = 60_000;

/// Connect N websockets in batches, returning established connections.
/// For N > PORTS_PER_IP, automatically uses multiple loopback source IPs
/// (127.0.0.1, 127.0.0.2, ...) to avoid ephemeral port exhaustion.
pub async fn connect_batch(
    host: &str,
    port: u16,
    token: &str,
    n: usize,
    batch_size: usize,
    uri_params: &str,
) -> Vec<WsStream> {
    let use_multi_ip = n > PORTS_PER_IP && host == "127.0.0.1";
    let mut connections = Vec::with_capacity(n);
    let mut errors = 0usize;
    let mut conn_idx = 0usize;

    for start in (0..n).step_by(batch_size) {
        let end = (start + batch_size).min(n);
        let chunk = end - start;

        let mut handles = Vec::with_capacity(chunk);
        for _ in 0..chunk {
            let host = host.to_string();
            let token = token.to_string();
            let params = uri_params.to_string();

            if use_multi_ip {
                // Distribute connections across 127.0.0.X source IPs
                let ip_idx = (conn_idx / PORTS_PER_IP) as u8 + 1; // 1, 2, 3, ...
                let source_ip: SocketAddr =
                    format!("127.0.0.{}:0", ip_idx).parse().unwrap();
                conn_idx += 1;
                handles.push(tokio::spawn(async move {
                    connect_and_handshake_from(
                        &host, port, &token, &params, source_ip, 15,
                    )
                    .await
                }));
            } else {
                handles.push(tokio::spawn(async move {
                    connect_and_handshake(&host, port, &token, &params, 15).await
                }));
            }
        }

        let results = futures_util::future::join_all(handles).await;
        for r in results {
            match r {
                Ok(Ok(ws)) => connections.push(ws),
                _ => errors += 1,
            }
        }

        eprint!("\r    Connecting: {}/{}...", connections.len(), n);

        if end < n {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    if use_multi_ip {
        let ips_used = (n / PORTS_PER_IP) + 1;
        eprintln!(
            "\r    Connected: {}/{} ({} failed, {} source IPs)            ",
            connections.len(),
            n,
            errors,
            ips_used
        );
    } else {
        eprintln!(
            "\r    Connected: {}/{} ({} failed)            ",
            connections.len(),
            n,
            errors
        );
    }

    connections
}

/// Close all connections gracefully.
pub async fn close_all(connections: Vec<WsStream>) {
    let mut handles = Vec::with_capacity(connections.len());
    for mut ws in connections {
        handles.push(tokio::spawn(async move {
            let _ = ws.close(None).await;
        }));
    }
    let _ = futures_util::future::join_all(handles).await;
}
