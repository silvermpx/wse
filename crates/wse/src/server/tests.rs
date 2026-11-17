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
fn a_stamped_message_says_its_position() {
    let stamped = stamp_recovery_fields(r#"{"c":"U","t":"x","p":{}}"#, "ro\"om", 0xab, 41);
    let prefix = super::fanout::stamp_prefix("ro\"om");
    assert_eq!(
        super::fanout::stamped_position(stamped.as_bytes(), &prefix),
        Some((0xab, 41))
    );
    // Another topic's, or one not stamped: none.
    let other = super::fanout::stamp_prefix("room");
    assert_eq!(
        super::fanout::stamped_position(stamped.as_bytes(), &other),
        None
    );
    assert_eq!(
        super::fanout::stamped_position(b"{\"t\":\"x\"}", &prefix),
        None
    );
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
        out: Arc::default(),
        closer: super::Closer::new(),
        msgpack: false,
        lane: 0,
    };
    (h, rx)
}

pub(super) fn test_handle() -> (ConnectionHandle, mpsc::UnboundedReceiver<WsFrame>) {
    make_handle()
}

fn queued(h: &ConnectionHandle) -> Vec<bytes::Bytes> {
    {
        let mut frames = std::collections::VecDeque::new();
        h.out.frames.lock().swap_into(&mut frames);
        frames.into_iter().collect()
    }
}

// -- Topic index ------------------------------------------------------------

#[test]
fn patterns_are_tracked_with_their_subscribers() {
    let index = TopicIndex::new(1);
    let (h, _rx) = make_handle();
    assert!(index.subscribe("news.*", "c1", &h));
    assert!(!index.subscribe("news.*", "c1", &h));
    index.subscribe("news.*", "c2", &h);
    index.subscribe("exact", "c1", &h);
    assert_eq!(index.pattern_count(), 1);
    assert!(!index.unsubscribe("news.*", "c1"));
    assert!(index.unsubscribe("news.*", "c2"));
    assert_eq!(index.pattern_count(), 0);
    assert!(index.subscribers.get("news.*").is_none());
}

#[test]
fn a_message_reaches_every_matching_pattern_wherever_it_is_kept() {
    let index = TopicIndex::new(1);
    let patterns = [
        "user:1:*",
        "user:*",
        "*",
        "*.btc",
        "pri?es.btc",
        "prices.b*",
        "user:2:*",
        "news.*",
    ];
    let handles: Vec<_> = patterns
        .iter()
        .enumerate()
        .map(|(i, pattern)| {
            let (h, rx) = make_handle();
            index.subscribe(pattern, &format!("c{i}"), &h);
            (h, rx)
        })
        .collect();
    let drops = AtomicU64::new(0);
    // queued() takes the frames: each topic is checked on empty queues.
    let reached = |topic: &str| -> Vec<&str> {
        index.fanout(
            topic,
            &Outgoing::text(bytes::Bytes::from_static(b"{}")),
            0,
            &drops,
        );
        patterns
            .iter()
            .zip(&handles)
            .filter(|(_, (h, _))| !queued(h).is_empty())
            .map(|(pattern, _)| *pattern)
            .collect()
    };
    assert_eq!(reached("user:1:orders"), ["user:1:*", "user:*", "*"]);
    assert_eq!(
        reached("prices.btc"),
        ["*", "*.btc", "pri?es.btc", "prices.b*"]
    );
    assert_eq!(reached("news"), ["*"]);
}

#[test]
fn a_pattern_subscriber_gets_only_what_its_acl_allows() {
    let index = TopicIndex::new(1);
    let (h, _rx) = make_handle();
    index.subscribe("tenant:acme:*", "c1", &h);
    index.acls.insert(
        "c1".into(),
        crate::server::TopicAcl {
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
    let index = TopicIndex::new(1);
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
    let index = TopicIndex::new(1);
    let (plain, _rx1) = make_handle();
    let (sealed, _rx2) = make_handle();
    let key = Arc::new(crate::crypto::Aes256Gcm::new(&[9u8; 32]).unwrap());
    sealed
        .out
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

    let index = TopicIndex::new(1);
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
        crate::msgpack::rmpv_to_serde_json(&value),
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

/// Take the bytes queued for `h` until there are `len` of them (its lane
/// delivered them), or five seconds have passed. A lane counts a frame in
/// `pending` just before it queues it, so the count alone does not say the
/// frame is there yet.
async fn delivered(h: &ConnectionHandle, len: usize) -> Vec<u8> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut bytes = Vec::with_capacity(len);
    loop {
        for frame in queued(h) {
            bytes.extend_from_slice(&frame);
        }
        if bytes.len() >= len || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::task::yield_now().await;
    }
    h.out.pending.fetch_sub(bytes.len(), Ordering::Relaxed);
    bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_fanout_reaches_everyone_once_and_in_order() {
    let index = TopicIndex::new(4);
    let handles: Vec<_> = (0..3000)
        .map(|i| {
            let (mut h, rx) = make_handle();
            h.lane = index.next_lane();
            let id = format!("c{i}");
            index.join(&id, &h);
            index.subscribe("big", &id, &h);
            (h, rx)
        })
        .collect();
    let drops = Arc::new(Tally::new(index.lanes()));
    let fanout = Fanout::start(index.clone(), 0, Arc::clone(&drops));
    for n in 0..3 {
        let msg = || Outgoing::text(bytes::Bytes::from(format!("{{\"n\":{n}}}")));
        fanout.publish("big".to_owned(), msg, None).await;
    }
    // Each connection's queue holds the three frames, in order.
    let expected: Vec<u8> = (0..3)
        .flat_map(|n| encode_ws_frame(0x01, format!("{{\"n\":{n}}}").as_bytes()).to_vec())
        .collect();
    for (h, _) in &handles {
        assert_eq!(delivered(h, expected.len()).await, expected);
    }

    fanout
        .broadcast(Outgoing::text(bytes::Bytes::from_static(b"{}")))
        .await;
    let frame = encode_ws_frame(0x01, b"{}");
    for (h, _) in &handles {
        assert_eq!(delivered(h, frame.len()).await, frame.to_vec());
    }
    // A connection that left its lane gets no more broadcasts; a lane
    // without connections gets no job.
    assert_eq!(index.occupied.load(Ordering::Relaxed), 0b1111);
    index.leave("c0", handles[0].0.lane);
    fanout
        .broadcast(Outgoing::text(bytes::Bytes::from_static(b"{}")))
        .await;
    assert_eq!(delivered(&handles[1].0, frame.len()).await, frame.to_vec());
    assert!(queued(&handles[0].0).is_empty());
    assert_eq!(drops.sum(), 0);
    for (i, (h, _)) in handles.iter().enumerate().filter(|(_, (h, _))| h.lane == 1) {
        index.leave(&format!("c{i}"), h.lane);
    }
    assert_eq!(index.occupied.load(Ordering::Relaxed), 0b1101);
}

#[tokio::test(flavor = "current_thread")]
async fn a_full_lane_makes_the_sender_wait_and_loses_nothing() {
    use super::fanout::LANE_QUEUE;

    let index = TopicIndex::new(1);
    let (h, _rx) = make_handle();
    index.subscribe("t", "c", &h);
    let fanout = Fanout::start(index, 0, Arc::new(Tally::new(1)));
    let frame = |n: usize| encode_ws_frame(0x01, format!("{{\"n\":{n}}}").as_bytes());
    let msg = |n: usize| move || Outgoing::text(bytes::Bytes::from(format!("{{\"n\":{n}}}")));
    // On this one thread the lane runs only when the sender waits: the
    // queue fills up without it.
    for n in 0..LANE_QUEUE {
        fanout.publish("t".to_owned(), msg(n), None).await;
    }
    assert_eq!(fanout.room(0), 0);
    assert!(queued(&h).is_empty());
    // The next message waits for room, the lane makes it.
    let total = LANE_QUEUE + 10;
    for n in LANE_QUEUE..total {
        fanout.publish("t".to_owned(), msg(n), None).await;
    }
    let expected: Vec<u8> = (0..total).flat_map(|n| frame(n).to_vec()).collect();
    assert_eq!(delivered(&h, expected.len()).await, expected);
}

#[test]
fn subscribers_are_kept_by_lane() {
    let index = TopicIndex::new(8);
    let mut handles = Vec::new();
    for (i, lane) in [5u8, 1, 7, 1, 0].into_iter().enumerate() {
        let (mut h, rx) = make_handle();
        h.lane = lane;
        assert!(index.subscribe("t", &format!("c{i}"), &h));
        handles.push((h, rx));
    }
    let mask = |index: &TopicIndex| index.subscribers.get("t").map(|s| s.mask);
    assert_eq!(mask(&index), Some(0b1010_0011));
    assert_eq!(index.subscribers.get("t").unwrap().len(), 5);
    // Each lane delivers to its own subscribers only.
    let drops = AtomicU64::new(0);
    let msg = Outgoing::text(bytes::Bytes::from_static(b"{}"));
    assert_eq!(index.fanout("t", &msg, 0, &drops), (5, 0));
    assert!(handles.iter().all(|(h, _)| queued(h).len() == 1));
    // A lane's bit goes with its last subscriber.
    assert!(!index.unsubscribe("t", "c1"));
    assert_eq!(mask(&index), Some(0b1010_0011));
    assert!(!index.unsubscribe("t", "c3"));
    assert_eq!(mask(&index), Some(0b1010_0001));
    assert!(!index.unsubscribe("t", "c0"));
    assert_eq!(mask(&index), Some(0b1000_0001));
    assert!(!index.unsubscribe("t", "c9"));
    assert!(!index.unsubscribe("t", "c4"));
    assert_eq!(mask(&index), Some(0b1000_0000));
    assert_eq!(index.fanout("t", &msg, 0, &drops), (1, 0));
    assert_eq!(queued(&handles[2].0).len(), 1);
    assert!(index.unsubscribe("t", "c2"));
    assert!(index.subscribers.get("t").is_none());
}

#[test]
fn lanes_go_in_turn() {
    let index = TopicIndex::new(3);
    let lanes: Vec<u8> = (0..7).map(|_| index.next_lane()).collect();
    assert_eq!(lanes, [0, 1, 2, 0, 1, 2, 0]);
    assert_eq!(TopicIndex::new(1000).lanes(), 64);
    assert_eq!(TopicIndex::new(0).lanes(), 1);
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
                h.out.pending.fetch_add(data.len(), Ordering::Relaxed);
                let mut buf = h.out.frames.lock();
                buf.push(&data);
            }
        }
    }

    // Check each member got 3 messages worth of data
    let tg = queue_groups.get(&topic).unwrap();
    let g = tg.get("workers").unwrap();
    for (_, h) in &g.members {
        assert_eq!(
            h.out.pending.load(Ordering::Relaxed),
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

#[test]
fn a_tally_counts_on_shards_and_reads_their_sum() {
    let tally = Tally::new(4);
    // Lanes 4 to 7 are past the last shard: they count on the first.
    for lane in 0..8 {
        tally.add(lane, 1);
    }
    assert_eq!(tally.sum(), 8);
    assert_eq!(tally.shard(0).load(Ordering::Relaxed), 5);
    assert_eq!(tally.shard(3).load(Ordering::Relaxed), 1);
    // Shards never share a cache line.
    let (a, b) = (tally.shard(0), tally.shard(1));
    let gap = (b as *const AtomicU64 as usize).abs_diff(a as *const AtomicU64 as usize);
    assert!(gap >= 128);
}

#[test]
fn a_full_command_queue_refuses_the_applications_sends() {
    let shared = Arc::new(Config::default().into_state().unwrap());
    let (tx, _rx) = mpsc::unbounded_channel();
    let queued = Arc::new(AtomicUsize::new(state::COMMAND_QUEUE));
    shared
        .commands
        .store(Some(Arc::new(Commands::new(tx, Arc::clone(&queued)))));
    let publisher = Publisher::new(Arc::clone(&shared));
    assert!(publisher.is_full());
    assert_eq!(publisher.send("c", "x"), Err(Error::Full));
    assert_eq!(publisher.publish("t", "{}"), Err(Error::Full));
    assert_eq!(publisher.broadcast("{}"), Err(Error::Full));
    // The server's own commands still go.
    assert_eq!(publisher.disconnect("c"), Ok(()));
    // With room again, sends go.
    queued.store(0, Ordering::Relaxed);
    assert_eq!(publisher.send("c", "x"), Ok(()));
}

#[test]
fn an_event_refused_for_a_full_queue_goes_when_sent_again() {
    let shared = Arc::new(Config::default().into_state().unwrap());
    let (tx, mut rx) = mpsc::unbounded_channel();
    let queued = Arc::new(AtomicUsize::new(state::COMMAND_QUEUE));
    shared
        .commands
        .store(Some(Arc::new(Commands::new(tx, Arc::clone(&queued)))));
    shared
        .conn_last_activity
        .insert("c".to_owned(), Arc::new(state::Activity::default()));
    let publisher = Publisher::new(Arc::clone(&shared));
    let event = || {
        serde_json::json!({"t": "price", "id": "e1", "p": {}})
            .as_object()
            .unwrap()
            .clone()
    };
    assert_eq!(publisher.send_event("c", event(), 0), Err(Error::Full));
    // With room again, the event the queue refused goes, once.
    queued.store(0, Ordering::Relaxed);
    assert!(publisher.send_event("c", event(), 0).unwrap() > 0);
    assert_eq!(publisher.send_event("c", event(), 0), Ok(0));
    assert!(rx.try_recv().is_ok());
    assert!(rx.try_recv().is_err());
}

#[test]
fn a_message_published_while_a_connection_subscribes_reaches_it() {
    let state = Config {
        recovery: Some(RecoverySettings::default()),
        ..Config::default()
    }
    .into_state()
    .unwrap();
    let recovery = Arc::clone(state.recovery.as_ref().unwrap());
    let publish = |text: &str| {
        let stamped = recovery
            .push_stamped("t", |epoch, offset| {
                bytes::Bytes::from(fanout::stamp_recovery_fields(text, "t", epoch, offset))
            })
            .unwrap();
        let drops = AtomicU64::new(0);
        state
            .topics
            .fanout("t", &Outgoing::text(stamped), 0, &drops);
    };
    publish(r#"{"c":"U","t":"m","p":{"n":0}}"#);
    let (h, mut rx) = test_handle();
    let (_, outcome) = state
        .subscribe_and_recover_with(
            "c1",
            &h,
            vec!["t".to_owned()],
            &std::collections::HashMap::new(),
            // Subscribed, its streams not read yet.
            || publish(r#"{"c":"U","t":"m","p":{"n":1}}"#),
        )
        .unwrap();
    // The client starts where the reply says: a message at or before that
    // offset is one it has. The one published meanwhile it must have got
    // before the reply (control frames go out before broadcasts).
    let start = outcome["t"].streams.clone();
    let mut before_reply = Vec::new();
    while let Ok(frame) = rx.try_recv() {
        if let WsFrame::Msg(tokio_tungstenite::tungstenite::protocol::Message::Text(text)) = frame {
            before_reply.push(text.to_string());
        }
    }
    let at_or_before = |text: &str| {
        let value: serde_json::Value = serde_json::from_str(text).unwrap();
        let (epoch, offset) = (
            u32::from_str_radix(value["e"].as_str().unwrap(), 16).unwrap(),
            value["o"].as_u64().unwrap(),
        );
        start.iter().any(|(e, o)| *e == epoch && offset <= *o)
    };
    // Every message after the subscription the client gets: live, or, at or
    // before where it starts, before the reply.
    for live in queued(&h) {
        let text = std::str::from_utf8(&live[2..]).unwrap();
        if at_or_before(text) {
            assert!(
                before_reply.iter().any(|t| t == text),
                "{text} is at or before the start {start:?} and came only live"
            );
        }
    }
}
