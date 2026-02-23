# Hacker News — Show HN Post

**Title:** Show HN: WSE – Rust-accelerated WebSocket engine for Python (2M msg/s)

**URL:** https://github.com/silvermpx/wse

---

## First Comment

author here. i work on real-time systems — trading and encrypted service-to-service messaging. got fed up with python websocket performance so i built this.

wse embeds into fastapi via pyo3. your app logic stays in python, the transport layer (tcp accept, framing, compression, encryption, ping/pong) runs in rust. python never touches raw frames.

two things that made the biggest difference:

1) jwt validation in rust during the ws handshake. cookie parsing, hs256 sig check, expiry — all before python knows someone connected. went from 23ms to 0.53ms per connection.

2) drain mode — rust buffers inbound messages, python pulls in batches. one gil acquire per batch instead of per message. outbound does the same — write coalescing, up to 64 msgs per syscall.

numbers: 2M msg/s sustained on epyc 7502p (64 workers), 500k on macbook m2 (10 workers). all json — msgpack is about the same since parsing happens in rust either way.

also has e2e encryption (ecdh p-256 + aes-gcm-256), zlib compression, circuit breaker, rate limiting, pub/sub with redis fanout.

clients for typescript/react and python. 376+ tests, ci on python 3.12-3.14.

happy to answer anything about the architecture or the optimization journey.

---

## Timing

Best time: Monday-Tuesday, 9-11 AM EST (17:00-19:00 MSK)
