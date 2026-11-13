use super::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use dashmap::DashMap;
use tokio::sync::mpsc;

use super::frames::{encode_ws_frame, pre_frame_binary};
use super::handshake::HandshakeGuard;

#[test]
fn stamp_recovery_fields_injects_tp_e_o_and_preserves_payload() {
    let stamped = stamp_recovery_fields(r#"{"c":"U","t":"x","p":{"k":1},"v":1}"#, "room", 0xab, 7);
    let v: serde_json::Value = serde_json::from_str(&stamped).expect("valid JSON");
    assert_eq!(v["tp"], "room");
    assert_eq!(v["e"], "000000ab"); // 8-hex, matches the recover() epoch format
    assert_eq!(v["o"], 7);
    // Original envelope fields are preserved.
    assert_eq!(v["c"], "U");
    assert_eq!(v["t"], "x");
    assert_eq!(v["p"]["k"], 1);
    assert_eq!(v["v"], 1);
}

#[test]
fn stamp_recovery_fields_handles_empty_object_escape_and_non_object() {
    // Empty object -> no dangling comma.
    let s = stamp_recovery_fields("{}", "t", 1, 2);
    let v: serde_json::Value = serde_json::from_str(&s).expect("valid JSON");
    assert_eq!(v["tp"], "t");
    assert_eq!(v["o"], 2);
    // A topic containing a quote is JSON-escaped (no corruption).
    let s2 = stamp_recovery_fields(r#"{"a":1}"#, "ro\"om", 0, 0);
    let v2: serde_json::Value = serde_json::from_str(&s2).expect("valid JSON");
    assert_eq!(v2["tp"], "ro\"om");
    assert_eq!(v2["a"], 1);
    // Non-object input is returned unchanged (defensive).
    assert_eq!(stamp_recovery_fields("[1,2]", "t", 1, 2), "[1,2]");
}

// The slowloris cap relies on HandshakeGuard counting in-flight handshakes
// exactly: +1 on construction, -1 on drop (handshake complete / failed).
#[test]
fn handshake_guard_counts_in_flight() {
    let counter = Arc::new(AtomicUsize::new(0));
    {
        let _g1 = HandshakeGuard::new(counter.clone());
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        {
            let _g2 = HandshakeGuard::new(counter.clone());
            assert_eq!(counter.load(Ordering::Relaxed), 2);
        }
        assert_eq!(counter.load(Ordering::Relaxed), 1, "inner slot released");
    }
    assert_eq!(counter.load(Ordering::Relaxed), 0, "all slots released");
}

#[test]
fn test_encode_ws_frame_small() {
    // Text frame, 5-byte payload "hello"
    let frame = encode_ws_frame(0x01, b"hello");
    assert_eq!(frame[0], 0x81); // FIN=1 + opcode=text
    assert_eq!(frame[1], 5); // payload length
    assert_eq!(&frame[2..], b"hello");
    assert_eq!(frame.len(), 7); // 2-byte header + 5 payload
}

#[test]
fn test_encode_ws_frame_medium() {
    // 200-byte payload (uses 2-byte extended length)
    let data = vec![0x42u8; 200];
    let frame = encode_ws_frame(0x02, &data);
    assert_eq!(frame[0], 0x82); // FIN=1 + opcode=binary
    assert_eq!(frame[1], 126); // extended 16-bit length marker
    assert_eq!(u16::from_be_bytes([frame[2], frame[3]]), 200);
    assert_eq!(&frame[4..], &data[..]);
    assert_eq!(frame.len(), 204); // 4-byte header + 200 payload
}

#[test]
fn test_encode_ws_frame_large() {
    // 70000-byte payload (uses 8-byte extended length)
    let data = vec![0xAA; 70000];
    let frame = encode_ws_frame(0x01, &data);
    assert_eq!(frame[0], 0x81);
    assert_eq!(frame[1], 127); // extended 64-bit length marker
    let len = u64::from_be_bytes(frame[2..10].try_into().unwrap());
    assert_eq!(len, 70000);
    assert_eq!(frame.len(), 10 + 70000);
}

#[test]
fn test_pre_frame_text() {
    let frame = pre_frame_text("hi");
    assert_eq!(frame[0], 0x81); // text opcode
    assert_eq!(frame[1], 2);
    assert_eq!(&frame[2..], b"hi");
}

#[test]
fn test_pre_frame_binary() {
    let frame = pre_frame_binary(&[1, 2, 3]);
    assert_eq!(frame[0], 0x82); // binary opcode
    assert_eq!(frame[1], 3);
    assert_eq!(&frame[2..], &[1, 2, 3]);
}

// -- Queue group tests ----------------------------------------------------

fn make_handle() -> (ConnectionHandle, mpsc::UnboundedReceiver<WsFrame>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let h = ConnectionHandle {
        tx,
        pending: Arc::new(AtomicUsize::new(0)),
        broadcast_buf: Arc::new(parking_lot::Mutex::new(BroadcastQueue::new())),
        broadcast_notify: Arc::new(tokio::sync::Notify::new()),
        closer: super::Closer::new(),
        cipher: Arc::new(std::sync::OnceLock::new()),
        msgpack: false,
    };
    (h, rx)
}

fn queued(h: &ConnectionHandle) -> Vec<bytes::Bytes> {
    h.broadcast_buf.lock().take().into_iter().collect()
}

// -- Topic index ------------------------------------------------------------

#[test]
fn patterns_are_tracked_with_their_subscribers() {
    let index = TopicIndex::default();
    let (h, _rx) = make_handle();
    assert!(index.subscribe("news.*", "c1", &h));
    assert!(!index.subscribe("news.*", "c1", &h));
    index.subscribe("news.*", "c2", &h);
    index.subscribe("exact", "c1", &h);
    assert_eq!(index.patterns.len(), 1);
    assert!(!index.unsubscribe("news.*", "c1"));
    assert!(index.unsubscribe("news.*", "c2"));
    assert!(index.patterns.is_empty());
    assert!(index.subscribers.get("news.*").is_none());
}

#[test]
fn a_pattern_subscriber_gets_only_what_its_acl_allows() {
    let index = TopicIndex::default();
    let (h, _rx) = make_handle();
    index.subscribe("tenant:acme:*", "c1", &h);
    index.acls.insert(
        "c1".into(),
        crate::wse::server::TopicAcl {
            allow: vec!["tenant:acme:*".into()],
            deny: vec!["tenant:acme:internal:*".into()],
        },
    );
    let drops = AtomicU64::new(0);
    let msg = Outgoing::text(bytes::Bytes::from_static(b"{}"));
    assert_eq!(index.fanout("tenant:acme:orders", &msg, 0, &drops), (1, 0));
    assert_eq!(
        index.fanout("tenant:acme:internal:logs", &msg, 0, &drops),
        (0, 0)
    );
    assert_eq!(queued(&h).len(), 1);
}

#[test]
fn a_connection_subscribed_twice_gets_a_message_once() {
    let index = TopicIndex::default();
    let (h, _rx) = make_handle();
    index.subscribe("room.1", "c1", &h);
    index.subscribe("room.*", "c1", &h);
    let drops = AtomicU64::new(0);
    let msg = Outgoing::text(bytes::Bytes::from_static(b"{}"));
    assert_eq!(index.fanout("room.1", &msg, 0, &drops), (1, 0));
    assert_eq!(queued(&h).iter().map(|b| b.len()).sum::<usize>(), 4);
}

#[test]
fn an_encrypted_connection_gets_its_own_sealed_frame() {
    let index = TopicIndex::default();
    let (plain, _rx1) = make_handle();
    let (sealed, _rx2) = make_handle();
    let key = Arc::new(crate::crypto::Aes256Gcm::new(&[9u8; 32]).unwrap());
    sealed
        .cipher
        .set(super::state::Seal::Legacy(key.clone()))
        .ok()
        .unwrap();
    index.subscribe("t", "plain", &plain);
    index.subscribe("t", "sealed", &sealed);
    let drops = AtomicU64::new(0);
    index.fanout(
        "t",
        &Outgoing::text(bytes::Bytes::from_static(b"{\"x\":1}")),
        0,
        &drops,
    );
    let plain_frame = queued(&plain).concat();
    assert_eq!(&plain_frame[2..], b"{\"x\":1}");
    let sealed_frame = queued(&sealed).concat();
    assert_eq!(sealed_frame[0], 0x82); // binary
    let body = &sealed_frame[2..];
    assert_eq!(&body[..2], b"E:");
    assert_eq!(key.open_nonce_prefixed(&body[2..]).unwrap(), b"{\"x\":1}");
}

#[test]
fn a_msgpack_connection_gets_json_as_msgpack() {
    use tokio_tungstenite::tungstenite::protocol::Message;

    let index = TopicIndex::default();
    let (json, _rx1) = make_handle();
    let (mut packed, mut rx2) = make_handle();
    packed.msgpack = true;
    index.subscribe("t", "json", &json);
    index.subscribe("t", "packed", &packed);
    let drops = AtomicU64::new(0);
    let body = br#"{"t":"tick","p":{"n":1}}"#;
    index.fanout(
        "t",
        &Outgoing::text(bytes::Bytes::from_static(body)),
        0,
        &drops,
    );

    let json_frame = queued(&json).concat();
    assert_eq!(json_frame[0], 0x81); // text
    assert_eq!(&json_frame[2..], body);
    let packed_frame = queued(&packed).concat();
    assert_eq!(packed_frame[0], 0x82); // binary
    assert_eq!(&packed_frame[2..4], b"M:");
    let value = rmpv::decode::read_value(&mut &packed_frame[4..]).unwrap();
    assert_eq!(
        crate::wse::msgpack::rmpv_to_serde_json(&value),
        serde_json::json!({"t": "tick", "p": {"n": 1}})
    );

    // Bytes the application sent as bytes go as they are.
    index.fanout(
        "t",
        &Outgoing::binary(bytes::Bytes::from_static(b"raw")),
        0,
        &drops,
    );
    assert_eq!(&queued(&packed).concat()[2..], b"raw");

    // Its own sends too (a replay, a reply); text that is not JSON as is.
    assert!(packed.send_one(Message::Text("{\"t\":\"x\"}".into()), 0, &drops));
    assert!(packed.send_one(Message::Text("plain".into()), 0, &drops));
    match rx2.try_recv() {
        Ok(WsFrame::Msg(Message::Binary(b))) => assert_eq!(&b[..2], b"M:"),
        _ => panic!("expected a msgpack frame"),
    }
    match rx2.try_recv() {
        Ok(WsFrame::Msg(Message::Text(t))) => assert_eq!(t.as_str(), "plain"),
        _ => panic!("expected the text as is"),
    }
}

#[test]
fn unstamped_removes_exactly_the_recovery_fields() {
    let original = r#"{"c":"U","t":"x","p":{"k":"a\"b"}}"#;
    let stamped = stamp_recovery_fields(original, "ro\"om", 0xab, 42);
    assert_eq!(
        unstamped(&bytes::Bytes::from(stamped)),
        bytes::Bytes::from(original)
    );
    let empty = stamp_recovery_fields("{}", "t", 1, 2);
    assert_eq!(
        unstamped(&bytes::Bytes::from(empty)),
        bytes::Bytes::from("{}")
    );
    let plain = bytes::Bytes::from_static(br#"{"tp":"look-alike"}"#);
    assert_eq!(unstamped(&plain), plain);
}

#[test]
fn test_queue_group_round_robin() {
    // 3 members in one group, 9 messages -> each gets 3
    let queue_groups: DashMap<String, DashMap<String, QueueGroup>> = DashMap::new();
    let topic = "work".to_string();

    let (h1, _rx1) = make_handle();
    let (h2, _rx2) = make_handle();
    let (h3, _rx3) = make_handle();

    let group = QueueGroup {
        members: vec![
            ("c1".to_string(), h1),
            ("c2".to_string(), h2),
            ("c3".to_string(), h3),
        ],
        next: AtomicUsize::new(0),
    };
    let inner: DashMap<String, QueueGroup> = DashMap::new();
    inner.insert("workers".to_string(), group);
    queue_groups.insert(topic.clone(), inner);

    let data = pre_frame_text("hello");
    // Send 9 messages through round-robin
    for _ in 0..9 {
        if let Some(tg) = queue_groups.get(&topic) {
            for mut entry in tg.iter_mut() {
                let g = entry.value_mut();
                let idx = g.next.fetch_add(1, Ordering::Relaxed) % g.members.len();
                let (_, ref h) = g.members[idx];
                h.pending.fetch_add(data.len(), Ordering::Relaxed);
                let mut buf = h.broadcast_buf.lock();
                buf.push(&data);
            }
        }
    }

    // Check each member got 3 messages worth of data
    let tg = queue_groups.get(&topic).unwrap();
    let g = tg.get("workers").unwrap();
    for (_, h) in &g.members {
        assert_eq!(
            h.pending.load(Ordering::Relaxed),
            data.len() * 3,
            "Each member should receive 3 messages"
        );
    }
}

#[test]
fn test_queue_group_and_fanout_coexist() {
    // Fan-out subscribers and queue group members can exist for the same topic
    let topic_subscribers: DashMap<String, DashMap<String, ConnectionHandle>> = DashMap::new();
    let queue_groups: DashMap<String, DashMap<String, QueueGroup>> = DashMap::new();
    let topic = "events".to_string();

    // Fan-out subscriber
    let (h_fanout, _rx_fanout) = make_handle();
    let inner_subs: DashMap<String, ConnectionHandle> = DashMap::new();
    inner_subs.insert("fanout1".to_string(), h_fanout);
    topic_subscribers.insert(topic.clone(), inner_subs);

    // Queue group member
    let (h_qg, _rx_qg) = make_handle();
    let group = QueueGroup {
        members: vec![("qg1".to_string(), h_qg)],
        next: AtomicUsize::new(0),
    };
    let inner_groups: DashMap<String, QueueGroup> = DashMap::new();
    inner_groups.insert("workers".to_string(), group);
    queue_groups.insert(topic.clone(), inner_groups);

    // Both should exist independently
    assert_eq!(topic_subscribers.get(&topic).unwrap().len(), 1);
    assert_eq!(queue_groups.get(&topic).unwrap().len(), 1);
}

#[test]
fn test_queue_group_member_disconnect() {
    let queue_groups: DashMap<String, DashMap<String, QueueGroup>> = DashMap::new();
    let conn_queue_groups: DashMap<String, Vec<(String, String)>> = DashMap::new();

    let (h1, _rx1) = make_handle();
    let (h2, _rx2) = make_handle();

    let group = QueueGroup {
        members: vec![("c1".to_string(), h1), ("c2".to_string(), h2)],
        next: AtomicUsize::new(0),
    };
    let inner: DashMap<String, QueueGroup> = DashMap::new();
    inner.insert("workers".to_string(), group);
    queue_groups.insert("work".to_string(), inner);

    conn_queue_groups.insert(
        "c1".to_string(),
        vec![("work".to_string(), "workers".to_string())],
    );

    // Simulate disconnect cleanup for c1
    if let Some((_, memberships)) = conn_queue_groups.remove("c1") {
        for (topic, group_name) in memberships {
            if let Some(tg) = queue_groups.get(&topic) {
                if let Some(mut g) = tg.get_mut(&group_name) {
                    g.members.retain(|(id, _)| id != "c1");
                }
                tg.remove_if(&group_name, |_, g| g.members.is_empty());
            }
            queue_groups.remove_if(&topic, |_, groups| groups.is_empty());
        }
    }

    // c2 should still be a member
    let tg = queue_groups.get("work").unwrap();
    let g = tg.get("workers").unwrap();
    assert_eq!(g.members.len(), 1);
    assert_eq!(g.members[0].0, "c2");
}

#[test]
fn test_queue_group_empty_cleanup() {
    let queue_groups: DashMap<String, DashMap<String, QueueGroup>> = DashMap::new();
    let (h1, _rx1) = make_handle();

    let group = QueueGroup {
        members: vec![("c1".to_string(), h1)],
        next: AtomicUsize::new(0),
    };
    let inner: DashMap<String, QueueGroup> = DashMap::new();
    inner.insert("workers".to_string(), group);
    queue_groups.insert("work".to_string(), inner);

    // Remove the only member
    {
        let tg = queue_groups.get("work").unwrap();
        let mut g = tg.get_mut("workers").unwrap();
        g.members.retain(|(id, _)| id != "c1");
    }
    // Clean up empty groups
    if let Some(tg) = queue_groups.get("work") {
        tg.remove_if("workers", |_, g| g.members.is_empty());
    }
    queue_groups.remove_if("work", |_, groups| groups.is_empty());

    // Everything should be cleaned up
    assert!(queue_groups.is_empty());
}

#[test]
fn test_topic_metrics_counting() {
    let counts: DashMap<String, AtomicU64> = DashMap::new();

    // First insert
    counts.insert("chat.general".to_string(), AtomicU64::new(1));

    // Subsequent increments
    if let Some(counter) = counts.get("chat.general") {
        counter.fetch_add(1, Ordering::Relaxed);
    }
    if let Some(counter) = counts.get("chat.general") {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    // Different topic
    counts.insert("events.system".to_string(), AtomicU64::new(1));

    assert_eq!(
        counts.get("chat.general").unwrap().load(Ordering::Relaxed),
        3
    );
    assert_eq!(
        counts.get("events.system").unwrap().load(Ordering::Relaxed),
        1
    );
}

#[test]
fn test_topic_metrics_top_n_limit() {
    let counts: DashMap<String, AtomicU64> = DashMap::new();

    // Insert 60 topics
    for i in 0..60 {
        counts.insert(format!("topic.{i}"), AtomicU64::new(i as u64));
    }

    // Simulate top-50 extraction
    let mut entries: Vec<(String, u64)> = counts
        .iter()
        .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
        .collect();
    entries.sort_unstable_by_key(|e| std::cmp::Reverse(e.1));
    entries.truncate(50);

    assert_eq!(entries.len(), 50);
    // Highest should be 59
    assert_eq!(entries[0].1, 59);
    // 50th should be 10 (topics 0-9 excluded)
    assert_eq!(entries[49].1, 10);
}
