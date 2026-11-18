use super::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use super::protocol::{MsgType, write_framed};

#[test]
fn test_encode_decode_msg() {
    let mut buf = BytesMut::new();
    encode_msg(&mut buf, "chat.general", "hello world");
    let frame = decode_frame(buf).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Msg {
            topic: "chat.general".into(),
            payload: Bytes::from("hello world"),
            recovery: None,
            arrival: None,
        }
    );
}

#[test]
fn test_encode_decode_msg_compressed() {
    // Small payload: should NOT be compressed (below threshold)
    let mut buf = BytesMut::new();
    encode_msg_compressed(&mut buf, "chat", "hello");
    let frame = decode_frame(buf).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Msg {
            topic: "chat".into(),
            payload: Bytes::from("hello"),
            recovery: None,
            arrival: None,
        }
    );

    // Large payload: should be compressed (must be >= 256 bytes)
    let big_payload = r#"{"user":"alice","message":"hello world, this is a longer message with repeated patterns for compression","timestamp":1234567890,"tags":["alpha","beta","gamma","delta","epsilon","zeta","eta","theta","iota","kappa"],"metadata":{"source":"api-gateway","version":"1.0.0","region":"us-east-1","cluster":"prod-main","environment":"production","extra":"additional padding data to ensure this payload exceeds the compression threshold of 256 bytes easily"}}"#;
    assert!(big_payload.len() >= COMPRESSION_THRESHOLD);
    let mut buf = BytesMut::new();
    encode_msg_compressed(&mut buf, "events.user", big_payload);
    // Verify the flags byte indicates compression
    assert_eq!(buf[0], MsgType::Msg as u8);
    assert_eq!(buf[1] & FLAG_COMPRESSED, FLAG_COMPRESSED);
    // Decode should produce the original payload
    let frame = decode_frame(buf).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Msg {
            topic: "events.user".into(),
            payload: Bytes::from(big_payload),
            recovery: None,
            arrival: None,
        }
    );
}

#[test]
fn test_decode_uncompressed_msg_with_zero_flags() {
    // Verify that uncompressed messages (flags=0) still decode correctly
    let mut buf = BytesMut::new();
    encode_msg(&mut buf, "topic", "payload");
    assert_eq!(buf[1], 0); // flags byte should be 0
    let frame = decode_frame(buf).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Msg {
            topic: "topic".into(),
            payload: Bytes::from("payload"),
            recovery: None,
            arrival: None,
        }
    );
}

#[test]
fn test_decode_rejects_decompression_bomb() {
    // Compress a payload larger than MAX_FRAME_SIZE (1MB)
    let big = vec![b'A'; MAX_FRAME_SIZE + 1];
    let compressed = zstd::bulk::compress(&big, 1).unwrap();
    // Repeated bytes compress very well, so this fits in a frame
    assert!(compressed.len() < MAX_FRAME_SIZE);

    let topic = b"bomb";
    let mut buf = BytesMut::new();
    buf.put_u8(MsgType::Msg as u8);
    buf.put_u8(FLAG_COMPRESSED);
    buf.put_u16_le(topic.len() as u16);
    buf.put_u32_le(compressed.len() as u32);
    buf.put_slice(topic);
    buf.put_slice(&compressed);

    // decode_frame should reject because decompressed size exceeds MAX_FRAME_SIZE
    assert!(decode_frame(buf).is_none());
}

#[test]
fn test_decode_rejects_corrupt_compressed_payload() {
    // Random bytes with FLAG_COMPRESSED set should fail gracefully
    let topic = b"test";
    let garbage = b"this is not valid zstd data at all";
    let mut buf = BytesMut::new();
    buf.put_u8(MsgType::Msg as u8);
    buf.put_u8(FLAG_COMPRESSED);
    buf.put_u16_le(topic.len() as u16);
    buf.put_u32_le(garbage.len() as u32);
    buf.put_slice(topic);
    buf.put_slice(garbage.as_slice());

    assert!(decode_frame(buf).is_none());
}

#[test]
fn test_compressed_flag_ignored_on_non_msg_frames() {
    // SUB frame with FLAG_COMPRESSED set -- flag should be ignored
    let mut buf = BytesMut::new();
    encode_sub(&mut buf, "chat.*");
    // Manually set the flags byte to FLAG_COMPRESSED
    buf[1] = FLAG_COMPRESSED;
    let frame = decode_frame(buf).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Sub {
            topic: "chat.*".into(),
        }
    );
}

#[test]
fn test_incompressible_payload_falls_back_to_uncompressed() {
    // Random-looking data above threshold that doesn't compress well
    // Use bytes that look random to zstd (high entropy)
    let mut payload = String::with_capacity(512);
    for i in 0..512u32 {
        // Mix of chars to create high-entropy data
        payload.push(char::from((33 + (i * 7 + i * i) % 94) as u8));
    }
    assert!(payload.len() >= COMPRESSION_THRESHOLD);

    let mut buf = BytesMut::new();
    encode_msg_compressed(&mut buf, "test", &payload);
    // If compression didn't help, flags should be 0 (uncompressed fallback)
    // The function should still produce a valid frame either way
    let frame = decode_frame(buf).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Msg {
            topic: "test".into(),
            payload: Bytes::from(payload),
            recovery: None,
            arrival: None,
        }
    );
}

#[test]
fn a_producer_state_says_what_the_peer_could_not_tell() {
    use crate::server::{Decided, Hole, Refusal};
    let decided = Decided {
        mark: 9,
        holes: vec![Hole {
            from: 4,
            to: 5,
            why: Refusal::Busy,
        }],
        forgot: 2,
        undecided: Some(9),
        // Past the mark: what it wrote when a node could not be asked.
        unsure: 12,
    };
    let mut buf = BytesMut::new();
    protocol::encode_producer_state(&mut buf, 7, Some(&decided));
    assert_eq!(
        decode_frame(buf).unwrap(),
        ClusterFrame::ProducerState {
            id: 7,
            decided: Some(decided),
        }
    );
    let mut buf = BytesMut::new();
    protocol::encode_producer_state(&mut buf, 8, None);
    assert_eq!(
        decode_frame(buf).unwrap(),
        ClusterFrame::ProducerState {
            id: 8,
            decided: None,
        }
    );
    // A bound past the mark is no answer.
    let bad = Decided {
        mark: 3,
        forgot: 4,
        ..Decided::default()
    };
    let mut buf = BytesMut::new();
    protocol::encode_producer_state(&mut buf, 9, Some(&bad));
    assert_eq!(decode_frame(buf), None);
}

#[test]
fn test_encode_decode_ping_pong() {
    let mut buf = BytesMut::new();
    encode_ping(&mut buf);
    assert_eq!(decode_frame(buf).unwrap(), ClusterFrame::Ping);

    let mut buf = BytesMut::new();
    encode_pong(&mut buf);
    assert_eq!(decode_frame(buf).unwrap(), ClusterFrame::Pong);
}

#[test]
fn test_encode_decode_hello() {
    let mut buf = BytesMut::new();
    encode_hello(&mut buf, "instance-abc-123", CAP_INTEREST_ROUTING);
    let frame = decode_frame(buf).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Hello {
            instance_id: "instance-abc-123".into(),
            protocol_version: 1,
            capabilities: CAP_INTEREST_ROUTING,
        }
    );
}

#[test]
fn test_encode_decode_shutdown() {
    let mut buf = BytesMut::new();
    encode_shutdown(&mut buf);
    assert_eq!(decode_frame(buf).unwrap(), ClusterFrame::Shutdown);
}

#[test]
fn test_decode_invalid_magic() {
    let mut buf = BytesMut::new();
    buf.put_u8(MsgType::Hello as u8);
    buf.put_u8(0);
    buf.put_u16_le(0);
    buf.put_u32_le(12);
    buf.put_slice(b"BAD\x00"); // wrong magic
    buf.put_u16_le(1);
    buf.put_u16_le(0);
    buf.put_u32_le(0);
    assert!(decode_frame(buf).is_none());
}

#[test]
fn test_decode_truncated_frame() {
    let mut buf = BytesMut::new();
    buf.put_u8(MsgType::Msg as u8);
    buf.put_u8(0);
    buf.put_u16_le(5);
    buf.put_u32_le(100); // claims 100 bytes payload
    buf.put_slice(b"hello"); // only 5 bytes topic, no payload
    assert!(decode_frame(buf).is_none());
}

#[test]
fn test_decode_unknown_type() {
    let mut buf = BytesMut::new();
    buf.put_u8(0xFF);
    buf.put_u8(0);
    buf.put_u16_le(0);
    buf.put_u32_le(0);
    assert_eq!(
        decode_frame(buf),
        Some(ClusterFrame::Unknown { msg_type: 0xFF })
    );
}

#[test]
fn test_dlq_push_and_drain() {
    let mut dlq = ClusterDlq::new(3);
    dlq.push(ClusterDlqEntry {
        topic: "a".into(),
        payload: "1".into(),
        peer_addr: "1.2.3.4".into(),
        error: "err".into(),
    });
    dlq.push(ClusterDlqEntry {
        topic: "b".into(),
        payload: "2".into(),
        peer_addr: "1.2.3.4".into(),
        error: "err".into(),
    });
    assert_eq!(dlq.len(), 2);
    let entries = dlq.drain_all();
    assert_eq!(entries.len(), 2);
    assert_eq!(dlq.len(), 0);
}

#[test]
fn test_dlq_evicts_oldest() {
    let mut dlq = ClusterDlq::new(2);
    for i in 0..3 {
        dlq.push(ClusterDlqEntry {
            topic: format!("t{i}"),
            payload: format!("{i}"),
            peer_addr: "x".into(),
            error: "e".into(),
        });
    }
    assert_eq!(dlq.len(), 2);
    let entries = dlq.drain_all();
    assert_eq!(entries[0].topic, "t1");
    assert_eq!(entries[1].topic, "t2");
}

#[test]
fn test_encode_decode_sub() {
    let mut buf = BytesMut::new();
    assert!(encode_sub(&mut buf, "chat.general"));
    let frame = decode_frame(buf).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Sub {
            topic: "chat.general".into(),
        }
    );
}

#[test]
fn test_encode_decode_unsub() {
    let mut buf = BytesMut::new();
    assert!(encode_unsub(&mut buf, "chat.general"));
    let frame = decode_frame(buf).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Unsub {
            topic: "chat.general".into(),
        }
    );
}

#[test]
fn test_encode_decode_resync_multiple() {
    let topics = vec![
        "chat.general".to_string(),
        "chat.private".to_string(),
        "events.system".to_string(),
    ];
    let frames = encode_resync(&topics);
    assert_eq!(frames.len(), 1);
    let frame = decode_frame(frames.into_iter().next().unwrap()).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Resync {
            topics,
            is_continuation: false,
            more: false,
        }
    );
}

#[test]
fn test_encode_decode_resync_empty() {
    let frames = encode_resync(&[]);
    assert_eq!(frames.len(), 1);
    let frame = decode_frame(frames.into_iter().next().unwrap()).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Resync {
            topics: Vec::new(),
            is_continuation: false,
            more: false,
        }
    );
}

// Every RESYNC frame, header included, fits the receiver's frame limit, even
// a payload just under the limit, which used to go out as one oversized frame.
#[test]
fn resync_frames_never_exceed_the_frame_limit() {
    for target in [
        MAX_FRAME_SIZE - 12,
        MAX_FRAME_SIZE - 8,
        MAX_FRAME_SIZE - 3,
        3 * MAX_FRAME_SIZE,
    ] {
        let mut topics = Vec::new();
        let mut len = 0;
        let mut i = 0;
        while len < target {
            let t = format!("topic.{i:06}");
            len += t.len() + 1;
            topics.push(t);
            i += 1;
        }
        let frames = encode_resync(&topics);
        let count = frames.len();
        let mut decoded = Vec::new();
        for (n, frame) in frames.into_iter().enumerate() {
            assert!(
                frame.len() <= MAX_FRAME_SIZE,
                "frame {n} of {target}: {}",
                frame.len()
            );
            match decode_frame(frame).unwrap() {
                ClusterFrame::Resync {
                    topics,
                    is_continuation,
                    more,
                } => {
                    assert_eq!(is_continuation, n > 0);
                    assert_eq!(more, n + 1 < count);
                    decoded.extend(topics);
                }
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(decoded, topics);
    }
}

#[test]
fn test_encode_decode_peer_announce() {
    let mut buf = BytesMut::new();
    encode_peer_announce(&mut buf, "10.0.0.5:9222");
    let frame = decode_frame(buf).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::PeerAnnounce {
            addr: "10.0.0.5:9222".into()
        }
    );
}

#[test]
fn test_encode_decode_peer_list() {
    let addrs = vec!["10.0.0.1:9222".to_string(), "10.0.0.2:9222".to_string()];
    let mut buf = BytesMut::new();
    encode_peer_list(&mut buf, &addrs);
    let frame = decode_frame(buf).unwrap();
    assert_eq!(frame, ClusterFrame::PeerList { addrs });
}

#[test]
fn test_encode_decode_peer_list_empty() {
    let mut buf = BytesMut::new();
    encode_peer_list(&mut buf, &[]);
    let frame = decode_frame(buf).unwrap();
    assert_eq!(frame, ClusterFrame::PeerList { addrs: Vec::new() });
}

#[test]
fn test_encode_decode_sub_glob_pattern() {
    let mut buf = BytesMut::new();
    assert!(encode_sub(&mut buf, "user:*:events"));
    let frame = decode_frame(buf).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Sub {
            topic: "user:*:events".into(),
        }
    );
}

#[test]
fn test_encode_decode_msg_empty() {
    let mut buf = BytesMut::new();
    encode_msg(&mut buf, "", "");
    let frame = decode_frame(buf).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Msg {
            topic: "".into(),
            payload: Bytes::from(""),
            recovery: None,
            arrival: None,
        }
    );
}

#[test]
fn test_encode_decode_msg_large_payload() {
    let topic = "benchmark.load";
    let payload = "x".repeat(100_000);
    let mut buf = BytesMut::new();
    encode_msg(&mut buf, topic, &payload);
    let frame = decode_frame(buf).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Msg {
            topic: topic.into(),
            payload: Bytes::from(payload),
            recovery: None,
            arrival: None,
        }
    );
}

#[test]
fn test_decode_too_short() {
    let buf = BytesMut::from(&[0u8; 4][..]);
    assert!(decode_frame(buf).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_two_peers_exchange_hello() {
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Peer A connects to listener
    let client_task = tokio::spawn(async move {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (mut read, mut write) = stream.into_split();

        // Send HELLO
        let mut hello = BytesMut::new();
        encode_hello(&mut hello, "peer-a", LOCAL_CAPABILITIES);
        let mut frame = BytesMut::new();
        write_framed(&mut frame, &hello);
        write.write_all(&frame).await.unwrap();

        // Read HELLO from peer B
        let mut len_buf = [0u8; 4];
        read.read_exact(&mut len_buf).await.unwrap();
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        let mut frame_buf = BytesMut::zeroed(frame_len);
        read.read_exact(&mut frame_buf).await.unwrap();

        decode_frame(frame_buf)
    });

    // Peer B accepts connection
    let (stream, _) = listener.accept().await.unwrap();
    let (mut read, mut write) = stream.into_split();

    // Read HELLO from peer A
    let mut len_buf = [0u8; 4];
    read.read_exact(&mut len_buf).await.unwrap();
    let frame_len = u32::from_be_bytes(len_buf) as usize;
    let mut frame_buf = BytesMut::zeroed(frame_len);
    read.read_exact(&mut frame_buf).await.unwrap();
    let hello_a = decode_frame(frame_buf).unwrap();

    assert!(
        matches!(hello_a, ClusterFrame::Hello { ref instance_id, .. } if instance_id == "peer-a")
    );

    // Send HELLO back
    let mut hello = BytesMut::new();
    encode_hello(&mut hello, "peer-b", LOCAL_CAPABILITIES);
    let mut frame = BytesMut::new();
    write_framed(&mut frame, &hello);
    write.write_all(&frame).await.unwrap();

    // Verify peer A received peer B's HELLO
    let hello_b = client_task.await.unwrap().unwrap();
    assert!(
        matches!(hello_b, ClusterFrame::Hello { ref instance_id, .. } if instance_id == "peer-b")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_msg_roundtrip() {
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let sender = tokio::spawn(async move {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (_, mut write) = stream.into_split();

        // Send a MSG frame
        let mut msg = BytesMut::new();
        encode_msg(&mut msg, "chat.general", r#"{"text":"hello"}"#);
        let mut frame = BytesMut::new();
        write_framed(&mut frame, &msg);
        write.write_all(&frame).await.unwrap();
    });

    let (stream, _) = listener.accept().await.unwrap();
    let (mut read, _) = stream.into_split();

    // Read MSG frame
    let mut len_buf = [0u8; 4];
    read.read_exact(&mut len_buf).await.unwrap();
    let frame_len = u32::from_be_bytes(len_buf) as usize;
    let mut frame_buf = BytesMut::zeroed(frame_len);
    read.read_exact(&mut frame_buf).await.unwrap();

    let msg = decode_frame(frame_buf).unwrap();
    assert_eq!(
        msg,
        ClusterFrame::Msg {
            topic: "chat.general".into(),
            payload: Bytes::from(r#"{"text":"hello"}"#),
            recovery: None,
            arrival: None,
        }
    );

    sender.await.unwrap();
}

#[test]
fn test_negotiate_version_same() {
    assert_eq!(negotiate_version(1, 1), Some(1));
}

#[test]
fn test_negotiate_version_different() {
    assert_eq!(negotiate_version(2, 1), Some(1));
    assert_eq!(negotiate_version(1, 2), Some(1));
}

#[test]
fn test_negotiate_version_zero_rejected() {
    assert_eq!(negotiate_version(1, 0), None);
}

#[test]
fn test_negotiate_capabilities() {
    assert_eq!(negotiate_capabilities(0b11, 0b10), 0b10);
    assert_eq!(negotiate_capabilities(0b01, 0b10), 0b00);
    assert_eq!(
        negotiate_capabilities(CAP_INTEREST_ROUTING, CAP_INTEREST_ROUTING | CAP_COMPRESSION),
        CAP_INTEREST_ROUTING
    );
}

#[test]
fn test_encode_decode_hello_with_capabilities() {
    let mut buf = BytesMut::new();
    encode_hello(
        &mut buf,
        "test-node",
        CAP_INTEREST_ROUTING | CAP_COMPRESSION,
    );
    let frame = decode_frame(buf).unwrap();
    assert_eq!(
        frame,
        ClusterFrame::Hello {
            instance_id: "test-node".into(),
            protocol_version: 1,
            capabilities: CAP_INTEREST_ROUTING | CAP_COMPRESSION,
        }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sub_unsub_resync_roundtrip() {
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Peer A sends SUB, UNSUB, RESYNC in a single write
    let sender = tokio::spawn(async move {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (_, mut write) = stream.into_split();

        let mut wire = BytesMut::new();

        // SUB for "chat.general"
        let mut sub = BytesMut::new();
        encode_sub(&mut sub, "chat.general");
        write_framed(&mut wire, &sub);

        // UNSUB for "chat.private"
        let mut unsub = BytesMut::new();
        encode_unsub(&mut unsub, "chat.private");
        write_framed(&mut wire, &unsub);

        // RESYNC with 3 topics
        let topics = vec!["a".into(), "b".into(), "c".into()];
        for frame in &encode_resync(&topics) {
            write_framed(&mut wire, frame);
        }

        write.write_all(&wire).await.unwrap();
    });

    // Peer B reads and verifies each frame
    let (stream, _) = listener.accept().await.unwrap();
    let (mut read, _) = stream.into_split();
    let mut len_buf = [0u8; 4];

    // Read SUB
    read.read_exact(&mut len_buf).await.unwrap();
    let frame_len = u32::from_be_bytes(len_buf) as usize;
    let mut frame_buf = BytesMut::zeroed(frame_len);
    read.read_exact(&mut frame_buf).await.unwrap();
    assert_eq!(
        decode_frame(frame_buf).unwrap(),
        ClusterFrame::Sub {
            topic: "chat.general".into()
        }
    );

    // Read UNSUB
    read.read_exact(&mut len_buf).await.unwrap();
    let frame_len = u32::from_be_bytes(len_buf) as usize;
    let mut frame_buf = BytesMut::zeroed(frame_len);
    read.read_exact(&mut frame_buf).await.unwrap();
    assert_eq!(
        decode_frame(frame_buf).unwrap(),
        ClusterFrame::Unsub {
            topic: "chat.private".into()
        }
    );

    // Read RESYNC
    read.read_exact(&mut len_buf).await.unwrap();
    let frame_len = u32::from_be_bytes(len_buf) as usize;
    let mut frame_buf = BytesMut::zeroed(frame_len);
    read.read_exact(&mut frame_buf).await.unwrap();
    assert_eq!(
        decode_frame(frame_buf).unwrap(),
        ClusterFrame::Resync {
            topics: vec!["a".into(), "b".into(), "c".into()],
            is_continuation: false,
            more: false,
        }
    );

    sender.await.unwrap();
}

#[test]
fn test_build_cluster_tls_missing_cert_file() {
    let result = build_cluster_tls(
        "/nonexistent/cert.pem",
        "/nonexistent/key.pem",
        "/nonexistent/ca.pem",
    );
    assert!(result.is_err());
    let err = result.err().unwrap().to_string();
    assert!(
        err.contains("cert file"),
        "Error should mention cert file, got: {err}"
    );
}

/// Generate a self-signed CA and a node cert signed by it.
/// Returns (ca_cert_pem, node_cert_pem, node_key_pem).
fn generate_test_certs() -> (String, String, String) {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};
    use std::net::IpAddr;

    // Generate CA
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "WSE Test CA");
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    // Generate node cert signed by CA with IP SAN for 127.0.0.1
    let mut node_params = CertificateParams::default();
    node_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "test-node");
    let ip: IpAddr = "127.0.0.1".parse().unwrap();
    node_params.subject_alt_names = vec![SanType::IpAddress(ip)];
    let node_key = KeyPair::generate().unwrap();
    let ca_issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
    let node_cert = node_params.signed_by(&node_key, &ca_issuer).unwrap();

    (ca_cert.pem(), node_cert.pem(), node_key.serialize_pem())
}

#[test]
fn test_build_cluster_tls_valid_certs() {
    let (ca_pem, cert_pem, key_pem) = generate_test_certs();

    let dir = std::env::temp_dir().join(format!("wse_tls_test_valid_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("ca.pem"), &ca_pem).unwrap();
    std::fs::write(dir.join("cert.pem"), &cert_pem).unwrap();
    std::fs::write(dir.join("key.pem"), &key_pem).unwrap();

    let result = build_cluster_tls(
        dir.join("cert.pem").to_str().unwrap(),
        dir.join("key.pem").to_str().unwrap(),
        dir.join("ca.pem").to_str().unwrap(),
    );
    assert!(
        result.is_ok(),
        "build_cluster_tls failed: {:?}",
        result.err()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_tls_cluster_hello_roundtrip() {
    let (ca_pem, cert_pem, key_pem) = generate_test_certs();

    let dir = std::env::temp_dir().join(format!("wse_tls_roundtrip_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("ca.pem"), &ca_pem).unwrap();
    std::fs::write(dir.join("cert.pem"), &cert_pem).unwrap();
    std::fs::write(dir.join("key.pem"), &key_pem).unwrap();

    let tls = build_cluster_tls(
        dir.join("cert.pem").to_str().unwrap(),
        dir.join("key.pem").to_str().unwrap(),
        dir.join("ca.pem").to_str().unwrap(),
    )
    .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let tls_server = tls.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut tls_stream = tls_server.acceptor.accept(stream).await.unwrap();

        // Read HELLO from client
        let mut len_buf = [0u8; 4];
        tokio::io::AsyncReadExt::read_exact(&mut tls_stream, &mut len_buf)
            .await
            .unwrap();
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        let mut frame_buf = BytesMut::zeroed(frame_len);
        tokio::io::AsyncReadExt::read_exact(&mut tls_stream, &mut frame_buf)
            .await
            .unwrap();
        let frame = decode_frame(frame_buf);
        assert!(
            matches!(frame, Some(ClusterFrame::Hello { .. })),
            "Expected Hello frame from client"
        );

        // Send HELLO back
        let mut hello_buf = BytesMut::new();
        encode_hello(&mut hello_buf, "server-id", LOCAL_CAPABILITIES);
        let mut framed = BytesMut::new();
        write_framed(&mut framed, &hello_buf);
        tokio::io::AsyncWriteExt::write_all(&mut tls_stream, &framed)
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::flush(&mut tls_stream)
            .await
            .unwrap();
    });

    let tls_client = tls;
    let client = tokio::spawn(async move {
        let stream = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::from(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
        let mut tls_stream = tls_client
            .connector
            .connect(server_name, stream)
            .await
            .unwrap();

        // Send HELLO
        let mut hello_buf = BytesMut::new();
        encode_hello(&mut hello_buf, "client-id", LOCAL_CAPABILITIES);
        let mut framed = BytesMut::new();
        write_framed(&mut framed, &hello_buf);
        tokio::io::AsyncWriteExt::write_all(&mut tls_stream, &framed)
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::flush(&mut tls_stream)
            .await
            .unwrap();

        // Read HELLO response
        let mut len_buf = [0u8; 4];
        tokio::io::AsyncReadExt::read_exact(&mut tls_stream, &mut len_buf)
            .await
            .unwrap();
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        let mut frame_buf = BytesMut::zeroed(frame_len);
        tokio::io::AsyncReadExt::read_exact(&mut tls_stream, &mut frame_buf)
            .await
            .unwrap();
        let frame = decode_frame(frame_buf);
        assert!(
            matches!(frame, Some(ClusterFrame::Hello { .. })),
            "Expected Hello frame from server"
        );
    });

    server.await.unwrap();
    client.await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_tls_rejects_wrong_ca() {
    // Generate two separate CAs, each with their own node cert
    let (ca_pem_a, cert_pem_a, key_pem_a) = generate_test_certs();
    let (ca_pem_b, _cert_pem_b, _key_pem_b) = generate_test_certs();

    let dir = std::env::temp_dir().join(format!("wse_tls_wrong_ca_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Server uses CA A certs
    std::fs::write(dir.join("ca_a.pem"), &ca_pem_a).unwrap();
    std::fs::write(dir.join("cert_a.pem"), &cert_pem_a).unwrap();
    std::fs::write(dir.join("key_a.pem"), &key_pem_a).unwrap();

    // Client trusts CA B (different CA)
    std::fs::write(dir.join("ca_b.pem"), &ca_pem_b).unwrap();

    let server_tls = build_cluster_tls(
        dir.join("cert_a.pem").to_str().unwrap(),
        dir.join("key_a.pem").to_str().unwrap(),
        dir.join("ca_a.pem").to_str().unwrap(),
    )
    .unwrap();

    // Client uses cert from CA A but trusts CA B -- server should reject
    let client_tls = build_cluster_tls(
        dir.join("cert_a.pem").to_str().unwrap(),
        dir.join("key_a.pem").to_str().unwrap(),
        dir.join("ca_b.pem").to_str().unwrap(),
    )
    .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        // TLS handshake should fail (client doesn't trust our CA)
        let result = server_tls.acceptor.accept(stream).await;
        // Server may or may not fail depending on who detects mismatch first
        result.is_err()
    });

    let client = tokio::spawn(async move {
        let stream = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::from(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
        let result = client_tls.connector.connect(server_name, stream).await;
        // Client should fail -- server cert not signed by CA B
        result.is_err()
    });

    let server_failed = server.await.unwrap();
    let client_failed = client.await.unwrap();
    // At least one side should detect the CA mismatch
    assert!(
        server_failed || client_failed,
        "Expected TLS handshake failure with wrong CA"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A TLS link between two configs over loopback: what each end got from the
/// other, or None for a side whose handshake failed.
async fn secret_link(
    server_tls: ClusterTlsConfig,
    client_tls: ClusterTlsConfig,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut tls = server_tls.acceptor.accept(stream).await.ok()?;
        tls.write_all(b"from the server").await.ok()?;
        tls.flush().await.ok()?;
        let mut got = vec![0u8; 15];
        tls.read_exact(&mut got).await.ok()?;
        Some(got)
    });
    let client = tokio::spawn(async move {
        let stream = TcpStream::connect(addr).await.unwrap();
        let name = ServerName::from(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
        let mut tls = client_tls.connector.connect(name, stream).await.ok()?;
        tls.write_all(b"from the client").await.ok()?;
        tls.flush().await.ok()?;
        let mut got = vec![0u8; 15];
        tls.read_exact(&mut got).await.ok()?;
        Some(got)
    });
    (server.await.unwrap(), client.await.unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nodes_with_the_same_secret_link_in_tls() {
    let secret = b"a cluster secret of at least thirty-two bytes";
    let (server, client) = secret_link(
        build_cluster_tls_from_secret(secret).unwrap(),
        build_cluster_tls_from_secret(secret).unwrap(),
    )
    .await;
    assert_eq!(server.as_deref(), Some(b"from the client".as_slice()));
    assert_eq!(client.as_deref(), Some(b"from the server".as_slice()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_without_the_secret_cannot_link() {
    let ours = b"a cluster secret of at least thirty-two bytes";
    let theirs = b"another secret, also of at least thirty-two bytes";
    // Neither way round: a dialer without it, nor a listener without it.
    for (server, client) in [(ours.as_slice(), theirs.as_slice()), (theirs, ours)] {
        let (server, client) = secret_link(
            build_cluster_tls_from_secret(server).unwrap(),
            build_cluster_tls_from_secret(client).unwrap(),
        )
        .await;
        assert_eq!((server, client), (None, None));
    }
}

#[test]
fn a_short_secret_is_refused() {
    let error = build_cluster_tls_from_secret(b"too short").err().unwrap();
    assert!(error.to_string().contains("at least 32"), "{error}");
}

/// Test peer exchange: Node B sends PEER_LIST containing Node C's address to Node A.
/// Verifies that PEER_ANNOUNCE and PEER_LIST are correctly exchanged during handshake.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_peer_exchange_discovery() {
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b_addr = listener.local_addr().unwrap();
    let c_addr = "192.168.1.100:9001"; // Node C's address (doesn't need to exist)

    // Node A: connect to B, exchange HELLO, then read PEER_ANNOUNCE + PEER_LIST
    let node_a = tokio::spawn(async move {
        let stream = TcpStream::connect(b_addr).await.unwrap();
        let (mut read, mut write) = stream.into_split();

        // Send HELLO
        let mut hello = BytesMut::new();
        encode_hello(&mut hello, "node-a", LOCAL_CAPABILITIES);
        let mut frame = BytesMut::new();
        write_framed(&mut frame, &hello);
        write.write_all(&frame).await.unwrap();

        // Read HELLO from B
        let mut len_buf = [0u8; 4];
        read.read_exact(&mut len_buf).await.unwrap();
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        let mut frame_buf = BytesMut::zeroed(frame_len);
        read.read_exact(&mut frame_buf).await.unwrap();
        let hello_b = decode_frame(frame_buf).unwrap();
        assert!(
            matches!(hello_b, ClusterFrame::Hello { ref instance_id, .. } if instance_id == "node-b")
        );

        // Read PEER_ANNOUNCE from B (B announces its own cluster address)
        read.read_exact(&mut len_buf).await.unwrap();
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        let mut frame_buf = BytesMut::zeroed(frame_len);
        read.read_exact(&mut frame_buf).await.unwrap();
        let announce = decode_frame(frame_buf).unwrap();

        // Read PEER_LIST from B
        read.read_exact(&mut len_buf).await.unwrap();
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        let mut frame_buf = BytesMut::zeroed(frame_len);
        read.read_exact(&mut frame_buf).await.unwrap();
        let peer_list = decode_frame(frame_buf).unwrap();

        (announce, peer_list)
    });

    // Node B: accept connection, exchange HELLO, send PEER_ANNOUNCE + PEER_LIST
    let (stream, _) = listener.accept().await.unwrap();
    let (mut read, mut write) = stream.into_split();

    // Read HELLO from A
    let mut len_buf = [0u8; 4];
    read.read_exact(&mut len_buf).await.unwrap();
    let frame_len = u32::from_be_bytes(len_buf) as usize;
    let mut frame_buf = BytesMut::zeroed(frame_len);
    read.read_exact(&mut frame_buf).await.unwrap();
    let hello_a = decode_frame(frame_buf).unwrap();
    assert!(
        matches!(hello_a, ClusterFrame::Hello { ref instance_id, .. } if instance_id == "node-a")
    );

    // Send HELLO back
    let mut hello = BytesMut::new();
    encode_hello(&mut hello, "node-b", LOCAL_CAPABILITIES);
    let mut frame = BytesMut::new();
    write_framed(&mut frame, &hello);
    write.write_all(&frame).await.unwrap();

    // Send PEER_ANNOUNCE (B's own cluster address)
    let b_cluster_addr = format!("127.0.0.1:{}", b_addr.port());
    let mut announce = BytesMut::new();
    encode_peer_announce(&mut announce, &b_cluster_addr);
    let mut frame = BytesMut::new();
    write_framed(&mut frame, &announce);
    write.write_all(&frame).await.unwrap();

    // Send PEER_LIST with C's address
    let addrs = vec![c_addr.to_string(), b_cluster_addr.clone()];
    let mut list = BytesMut::new();
    encode_peer_list(&mut list, &addrs);
    let mut frame = BytesMut::new();
    write_framed(&mut frame, &list);
    write.write_all(&frame).await.unwrap();

    // Verify what A received
    let (announce_frame, peer_list_frame) = node_a.await.unwrap();

    // Check PEER_ANNOUNCE
    assert_eq!(
        announce_frame,
        ClusterFrame::PeerAnnounce {
            addr: b_cluster_addr.clone()
        }
    );

    // Check PEER_LIST contains both C's address and B's address
    match peer_list_frame {
        ClusterFrame::PeerList { addrs } => {
            assert!(
                addrs.contains(&c_addr.to_string()),
                "PEER_LIST should contain Node C's address"
            );
            assert!(
                addrs.contains(&b_cluster_addr),
                "PEER_LIST should contain Node B's address"
            );
            assert_eq!(addrs.len(), 2);
        }
        other => panic!("Expected PeerList, got {:?}", other),
    }
}

#[test]
fn test_encode_decode_presence_update() {
    let mut buf = BytesMut::new();
    let ok = encode_presence_update(
        &mut buf,
        "room:lobby",
        "alice",
        0, // join
        r#"{"status":"online"}"#,
        1709000000000,
    );
    assert!(ok);
    let frame = decode_frame(buf).unwrap();
    match frame {
        ClusterFrame::PresenceUpdate {
            topic,
            user_id,
            action,
            data,
            updated_at,
        } => {
            assert_eq!(topic, "room:lobby");
            assert_eq!(user_id, "alice");
            assert_eq!(action, 0);
            assert_eq!(data, r#"{"status":"online"}"#);
            assert_eq!(updated_at, 1709000000000);
        }
        other => panic!("Expected PresenceUpdate, got {:?}", other),
    }
}

#[test]
fn test_encode_decode_presence_update_leave() {
    let mut buf = BytesMut::new();
    let ok = encode_presence_update(&mut buf, "chat:main", "bob", 1, "", 1709000001000);
    assert!(ok);
    let frame = decode_frame(buf).unwrap();
    match frame {
        ClusterFrame::PresenceUpdate {
            topic,
            user_id,
            action,
            data,
            updated_at,
        } => {
            assert_eq!(topic, "chat:main");
            assert_eq!(user_id, "bob");
            assert_eq!(action, 1);
            assert_eq!(data, "");
            assert_eq!(updated_at, 1709000001000);
        }
        other => panic!("Expected PresenceUpdate, got {:?}", other),
    }
}

#[test]
fn test_encode_decode_presence_full() {
    let json = r#"{"room:lobby":{"alice":{"data":{"status":"online"},"updated_at":123}}}"#;
    let mut buf = BytesMut::new();
    let ok = encode_presence_full(&mut buf, json);
    assert!(ok);
    let frame = decode_frame(buf).unwrap();
    match frame {
        ClusterFrame::PresenceFull { entries, flags } => {
            assert_eq!(entries, json);
            assert_eq!(flags, 0);
        }
        other => panic!("Expected PresenceFull, got {:?}", other),
    }
}

#[test]
fn test_encode_decode_presence_full_empty() {
    let mut buf = BytesMut::new();
    let ok = encode_presence_full(&mut buf, "{}");
    assert!(ok);
    let frame = decode_frame(buf).unwrap();
    match frame {
        ClusterFrame::PresenceFull { entries, .. } => {
            assert_eq!(entries, "{}");
        }
        other => panic!("Expected PresenceFull, got {:?}", other),
    }
}

#[test]
fn test_cap_presence_flag() {
    // Verify CAP_PRESENCE is included in LOCAL_CAPABILITIES
    assert_ne!(LOCAL_CAPABILITIES & CAP_PRESENCE, 0);
    // Verify it's a distinct flag
    assert_eq!(CAP_PRESENCE, 1 << 2);
    assert_ne!(CAP_PRESENCE, CAP_INTEREST_ROUTING);
    assert_ne!(CAP_PRESENCE, CAP_COMPRESSION);
}

#[test]
fn test_cap_recovery_flag() {
    assert_ne!(LOCAL_CAPABILITIES & CAP_RECOVERY, 0);
    assert_eq!(CAP_RECOVERY, 1 << 3);
    assert_ne!(CAP_RECOVERY, CAP_INTEREST_ROUTING);
    assert_ne!(CAP_RECOVERY, CAP_COMPRESSION);
    assert_ne!(CAP_RECOVERY, CAP_PRESENCE);
}

#[test]
fn test_encode_decode_msg_with_recovery() {
    let mut buf = BytesMut::new();
    let epoch: u32 = 12345;
    let offset: u64 = 67890;
    encode_msg_with_recovery(
        &mut buf,
        "events.topic",
        r#"{"data":"test"}"#,
        epoch,
        offset,
        false,
    );
    let frame = decode_frame(buf).unwrap();
    match frame {
        ClusterFrame::Msg {
            topic,
            payload,
            recovery,
            ..
        } => {
            assert_eq!(topic, "events.topic");
            assert_eq!(&payload[..], br#"{"data":"test"}"#);
            assert_eq!(recovery, Some((epoch, offset)));
        }
        _ => panic!("expected Msg frame"),
    }
}

#[test]
fn test_msg_recovery_backward_compat() {
    // Old-style MSG (no recovery flag) should decode with recovery: None
    let mut buf = BytesMut::new();
    encode_msg(&mut buf, "topic1", "payload");
    let frame = decode_frame(buf).unwrap();
    match frame {
        ClusterFrame::Msg { recovery, .. } => {
            assert_eq!(recovery, None);
        }
        _ => panic!("expected Msg frame"),
    }
}

#[test]
fn test_msg_recovery_flag_with_compression() {
    let mut buf = BytesMut::new();
    let epoch: u32 = 999;
    let offset: u64 = 42;
    // Large enough payload to actually compress
    let payload = "x".repeat(200);
    let compressed = encode_msg_with_recovery(
        &mut buf,
        "big.topic",
        &payload,
        epoch,
        offset,
        true, // try compress
    );

    let frame = decode_frame(buf).unwrap();
    match frame {
        ClusterFrame::Msg {
            topic,
            payload: decoded_payload,
            recovery,
            ..
        } => {
            assert_eq!(topic, "big.topic");
            assert_eq!(recovery, Some((epoch, offset)));
            if compressed {
                // If it actually compressed, payload should match after decode
                assert_eq!(decoded_payload.len(), payload.len());
            }
            assert_eq!(std::str::from_utf8(&decoded_payload).unwrap(), payload);
        }
        _ => panic!("expected Msg frame"),
    }
}

#[test]
fn a_resume_request_round_trips_in_frames_under_the_limit() {
    // None held: one frame, the last, naming nothing.
    let frames = encode_resume(&[], &[], None);
    assert_eq!(frames.len(), 1);
    assert_eq!(
        decode_frame(frames[0].clone()),
        Some(ClusterFrame::Resume {
            points: Vec::new(),
            topics: Vec::new(),
            heard: None,
            last: true
        })
    );
    // More than a frame holds: split, only the last flagged, nothing lost.
    let topic = "t".repeat(1000);
    let points: Vec<ResumePoint> = (0..3000u64)
        .map(|i| (format!("{topic}{i}"), i as u32, i * 7))
        .collect();
    let frames = encode_resume(&points, &[], None);
    assert!(frames.len() > 1);
    let mut got = Vec::new();
    for (i, frame) in frames.into_iter().enumerate() {
        assert!(frame.len() <= MAX_FRAME_SIZE);
        let Some(ClusterFrame::Resume { points, last, .. }) = decode_frame(frame) else {
            panic!("not a RESUME frame");
        };
        assert_eq!(last, got.len() + points.len() == 3000, "frame {i}");
        got.extend(points);
    }
    assert_eq!(got, points);
}

#[test]
fn a_resume_request_names_the_topics_after_the_streams() {
    let points = vec![("news".to_owned(), 7, 41)];
    let topics = vec![("news".to_owned(), 30_000), ("room".to_owned(), 5)];
    let frames = encode_resume(&points, &topics, None);
    assert_eq!(frames.len(), 2);
    assert_eq!(
        decode_frame(frames[0].clone()),
        Some(ClusterFrame::Resume {
            points,
            topics: Vec::new(),
            heard: None,
            last: false
        })
    );
    assert_eq!(
        decode_frame(frames[1].clone()),
        Some(ClusterFrame::Resume {
            points: Vec::new(),
            topics,
            heard: None,
            last: true
        })
    );
}

#[test]
fn a_truncated_resume_request_does_not_decode() {
    let mut frame = encode_resume(&[("topic".to_owned(), 1, 2)], &[], None).remove(0);
    let len = frame.len();
    frame.truncate(len - 3);
    let short = (frame.len() - 8) as u32;
    frame[4..8].copy_from_slice(&short.to_le_bytes());
    assert_eq!(decode_frame(frame), None);
}

#[test]
fn a_replayed_message_is_marked_and_keeps_its_position() {
    let mut buf = BytesMut::new();
    let payload = r#"{"t":"x","p":{"n":1}}"#.repeat(40);
    assert!(encode_replayed(&mut buf, "news", &payload, 9, 41));
    assert_eq!(
        decode_frame(buf),
        Some(ClusterFrame::Replayed {
            topic: "news".to_owned(),
            payload: Bytes::from(payload),
            position: (9, 41),
        })
    );
    // A live one stays a MSG.
    let mut buf = BytesMut::new();
    assert!(encode_msg_with_recovery(
        &mut buf, "news", "{}", 9, 42, true
    ));
    assert!(matches!(decode_frame(buf), Some(ClusterFrame::Msg { .. })));
}

#[test]
fn catch_up_is_offered_with_recovery_buffers_only() {
    assert_eq!(LOCAL_CAPABILITIES & CAP_RESUME, 0);
    assert_ne!(
        negotiate_capabilities(
            LOCAL_CAPABILITIES | CAP_RESUME,
            LOCAL_CAPABILITIES | CAP_RESUME
        ) & CAP_RESUME,
        0
    );
    assert_eq!(
        negotiate_capabilities(LOCAL_CAPABILITIES | CAP_RESUME, LOCAL_CAPABILITIES) & CAP_RESUME,
        0
    );
}

#[test]
fn test_encode_decode_drain() {
    let mut buf = BytesMut::new();
    encode_drain(&mut buf);
    let frame = decode_frame(buf).unwrap();
    assert_eq!(frame, ClusterFrame::Drain);
}

#[test]
fn test_cluster_info_peer_info() {
    let metrics = ClusterMetrics::new();

    // Insert a peer
    metrics.peer_info.insert(
        "192.168.1.1:7006".to_string(),
        PeerInfo {
            address: "192.168.1.1:7006".to_string(),
            instance_id: "node-abc-123".to_string(),
            capabilities: 0x0F,
            connected_at: 1700000000000,
        },
    );

    assert_eq!(metrics.peer_info.len(), 1);
    let entry = metrics.peer_info.get("192.168.1.1:7006").unwrap();
    assert_eq!(entry.address, "192.168.1.1:7006");
    assert_eq!(entry.instance_id, "node-abc-123");
    assert_eq!(entry.capabilities, 0x0F);
    assert_eq!(entry.connected_at, 1700000000000);
    drop(entry);

    // Insert a second peer
    metrics.peer_info.insert(
        "192.168.1.2:7007".to_string(),
        PeerInfo {
            address: "192.168.1.2:7007".to_string(),
            instance_id: "node-def-456".to_string(),
            capabilities: 0x1F,
            connected_at: 1700000001000,
        },
    );
    assert_eq!(metrics.peer_info.len(), 2);

    // Remove the first peer
    metrics.peer_info.remove("192.168.1.1:7006");
    assert_eq!(metrics.peer_info.len(), 1);
    assert!(metrics.peer_info.get("192.168.1.1:7006").is_none());
    assert!(metrics.peer_info.get("192.168.1.2:7007").is_some());

    // Remove the second peer
    metrics.peer_info.remove("192.168.1.2:7007");
    assert_eq!(metrics.peer_info.len(), 0);
}

#[test]
fn test_reload_cluster_tls() {
    // Generate two independent CA/cert pairs to verify the swap
    let (ca_pem1, cert_pem1, key_pem1) = generate_test_certs();
    let (ca_pem2, cert_pem2, key_pem2) = generate_test_certs();

    let dir = std::env::temp_dir().join(format!("wse_tls_reload_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Write first set of certs
    std::fs::write(dir.join("ca1.pem"), &ca_pem1).unwrap();
    std::fs::write(dir.join("cert1.pem"), &cert_pem1).unwrap();
    std::fs::write(dir.join("key1.pem"), &key_pem1).unwrap();

    // Write second set of certs
    std::fs::write(dir.join("ca2.pem"), &ca_pem2).unwrap();
    std::fs::write(dir.join("cert2.pem"), &cert_pem2).unwrap();
    std::fs::write(dir.join("key2.pem"), &key_pem2).unwrap();

    // Build initial config and wrap in ArcSwap
    let config1 = build_cluster_tls(
        dir.join("cert1.pem").to_str().unwrap(),
        dir.join("key1.pem").to_str().unwrap(),
        dir.join("ca1.pem").to_str().unwrap(),
    )
    .expect("first build_cluster_tls failed");

    let holder = Arc::new(arc_swap::ArcSwap::from_pointee(config1));

    // Take a snapshot before reload
    let snap_before = holder.load();

    // Build new config and swap
    let config2 = build_cluster_tls(
        dir.join("cert2.pem").to_str().unwrap(),
        dir.join("key2.pem").to_str().unwrap(),
        dir.join("ca2.pem").to_str().unwrap(),
    )
    .expect("second build_cluster_tls failed");

    holder.store(Arc::new(config2));

    // Take a snapshot after reload
    let snap_after = holder.load();

    // The two snapshots must point to different allocations (different configs)
    assert!(
        !Arc::ptr_eq(&snap_before, &snap_after),
        "ArcSwap should point to a new config after reload"
    );

    // The old snapshot is still valid (not dropped) -- proves in-flight
    // handshakes using the old config won't be affected by the swap
    drop(snap_before);
    drop(snap_after);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_cluster_queue_group_dispatch() {
    use crate::server::QueueGroup;

    // Create queue groups with 3 members
    let queue_groups: Arc<DashMap<String, DashMap<String, QueueGroup>>> = Arc::new(DashMap::new());

    let mut members = Vec::new();
    let mut _rxs = Vec::new();
    for i in 0..3 {
        let (tx, rx) = mpsc::unbounded_channel();
        let h = crate::server::ConnectionHandle {
            tx,
            out: Arc::default(),
            closer: crate::server::Closer::new(),
            msgpack: false,
            lane: 0,
        };
        members.push((format!("conn{i}"), h));
        _rxs.push(rx);
    }

    let group = QueueGroup {
        members,
        next: AtomicUsize::new(0),
    };
    let inner: DashMap<String, QueueGroup> = DashMap::new();
    inner.insert("workers".to_string(), group);
    queue_groups.insert("tasks".to_string(), inner);

    let frame_len = b"test-payload".len() + 2;
    let drops = Arc::new(crate::server::Tally::new(1));
    let fanout =
        crate::server::Fanout::start(crate::server::TopicIndex::new(1), 0, Arc::clone(&drops));
    for _ in 0..6 {
        let (delivered, dropped) = fanout
            .queue_groups(
                &queue_groups,
                "tasks",
                || crate::server::Outgoing::text(bytes::Bytes::from_static(b"test-payload")),
                0,
                drops.shard(0),
            )
            .await;
        assert_eq!((delivered, dropped), (1, 0));
    }

    // Each member should have received 2 messages (6 / 3 round-robin), once
    // its lane delivered them.
    let tg = queue_groups.get("tasks").unwrap();
    let g = tg.get("workers").unwrap();
    for (id, h) in &g.members {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while h.out.pending.load(Ordering::Relaxed) < frame_len * 2
            && tokio::time::Instant::now() < deadline
        {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            h.out.pending.load(Ordering::Relaxed),
            frame_len * 2,
            "{id} should have 2 messages"
        );
    }
}

#[test]
fn a_relayed_message_carries_its_arrival_number() {
    let mut buf = BytesMut::new();
    assert!(super::protocol::encode_msg_full(
        &mut buf,
        "news",
        "{}",
        Some((7, 3)),
        Some(41),
        false
    ));
    assert_eq!(
        decode_frame(buf),
        Some(ClusterFrame::Msg {
            topic: "news".into(),
            payload: Bytes::from("{}"),
            recovery: Some((7, 3)),
            arrival: Some(41),
        })
    );
    // With no position (its origin had no room to keep it), and compressed.
    let payload = "x".repeat(4096);
    let mut buf = BytesMut::new();
    assert!(super::protocol::encode_msg_full(
        &mut buf,
        "news",
        &payload,
        None,
        Some(42),
        true
    ));
    let Some(ClusterFrame::Msg {
        payload: got,
        recovery,
        arrival,
        ..
    }) = decode_frame(buf)
    else {
        panic!("not a MSG frame");
    };
    assert_eq!((recovery, arrival), (None, Some(42)));
    assert_eq!(got, Bytes::from(payload));
}

#[test]
fn a_catch_up_says_how_far_it_covers_and_a_request_what_was_heard() {
    let mut buf = BytesMut::new();
    super::protocol::encode_caught_up(&mut buf, 1234);
    assert_eq!(
        decode_frame(buf),
        Some(ClusterFrame::CaughtUp { through: 1234 })
    );
    let frames = encode_resume(&[], &[("news".to_owned(), 5)], Some(77));
    let heard: Vec<Option<u64>> = frames
        .into_iter()
        .map(|frame| match decode_frame(frame) {
            Some(ClusterFrame::Resume { heard, .. }) => heard,
            other => panic!("not a RESUME frame: {other:?}"),
        })
        .collect();
    assert!(heard.contains(&Some(77)), "{heard:?}");
}

#[test]
fn what_was_heard_of_a_peer_stops_at_a_shed_message_until_the_next_catch_up() {
    let heard = Heard::default();
    heard.live(3);
    heard.live(5);
    assert_eq!(heard.asked(), 5);
    heard.shed();
    heard.live(9);
    heard.caught_up(9);
    // The next catch-up starts before the one shed, and counts from there.
    assert_eq!(heard.asked(), 5);
    heard.caught_up(12);
    heard.live(13);
    assert_eq!(heard.asked(), 13);
}

#[test]
fn lost_frame_round_trips() {
    let mut buf = BytesMut::new();
    assert!(super::protocol::encode_lost(&mut buf, "prices:*"));
    let frame = decode_frame(buf);
    assert!(matches!(frame, Some(ClusterFrame::Lost { ref topic }) if topic == "prices:*"));
    let long = "t".repeat(u16::MAX as usize + 1);
    assert!(!super::protocol::encode_lost(&mut BytesMut::new(), &long));
}

// Regression: the inbound MSG dispatch channel is BOUNDED at
// PEER_DISPATCH_QUEUE_CAP and sheds via try_send when full, so a fast peer
// cannot grow an unbounded backlog -> OOM. peer_reader relies on exactly this
// "try_send returns Err when full" behavior to shed (bumping messages_dropped)
// instead of blocking the reader or growing memory without bound.
#[tokio::test]
async fn dispatch_channel_is_bounded_and_sheds_when_full() {
    let (tx, _rx) = mpsc::channel::<DispatchItem>(PEER_DISPATCH_QUEUE_CAP);
    // Fill to capacity with no consumer draining.
    for _ in 0..PEER_DISPATCH_QUEUE_CAP {
        assert!(
            tx.try_send(DispatchItem::Message(
                "t".to_string(),
                Bytes::from_static(b"p"),
                None,
                None
            ))
            .is_ok(),
            "sends up to the cap must succeed"
        );
    }
    // The next send must shed (Err), never block or grow the queue.
    assert!(
        tx.try_send(DispatchItem::Message(
            "t".to_string(),
            Bytes::from_static(b"p"),
            None,
            None
        ))
        .is_err(),
        "send past the bound must fail (shed), proving the queue is bounded"
    );
}

fn link(dialer: &str, session: u64) -> (Link, tokio_util::sync::CancellationToken) {
    link_heard(dialer, session, super::epoch_ms())
}

/// A link that last received anything at `heard` (epoch ms).
fn link_heard(
    dialer: &str,
    session: u64,
    heard: u64,
) -> (Link, tokio_util::sync::CancellationToken) {
    let cancel = tokio_util::sync::CancellationToken::new();
    (
        Link {
            dialer: dialer.to_string(),
            session,
            cancel: cancel.clone(),
            activity: Arc::new(AtomicU64::new(heard)),
        },
        cancel,
    )
}

// Any node may open the first link to another, whichever of the two is newer.
#[test]
fn the_first_link_to_an_instance_is_kept_whoever_dialed() {
    let links = Links::new();
    let (newer_dials, _) = link("02-newer", 1);
    assert_eq!(
        claim_link(&links, "01-older", "02-newer", newer_dials),
        Ok(())
    );
    let links_b = Links::new();
    let (older_dials, _) = link("01-older", 2);
    assert_eq!(
        claim_link(&links_b, "02-newer", "01-older", older_dials),
        Ok(())
    );
}

// Both nodes dial each other at once: each sees the same two links and keeps
// the one dialed by the lower id, so they agree.
#[test]
fn simultaneous_links_settle_on_the_lower_dialer_on_both_nodes() {
    // Node "a": the inbound from "b" registered first, then its own outbound.
    let on_a = Links::new();
    let (b_dialed, b_dialed_cancel) = link("b", 1);
    assert_eq!(claim_link(&on_a, "a", "b", b_dialed), Ok(()));
    let (a_dialed, _) = link("a", 2);
    assert_eq!(claim_link(&on_a, "a", "b", a_dialed), Ok(()));
    assert!(
        b_dialed_cancel.is_cancelled(),
        "the link dialed by b is closed on a"
    );
    assert_eq!(on_a.get("b").unwrap().dialer, "a");

    // Node "b": its own outbound registered first, then the inbound from "a".
    let on_b = Links::new();
    let (b_out, b_out_cancel) = link("b", 3);
    assert_eq!(claim_link(&on_b, "b", "a", b_out), Ok(()));
    let (a_in, _) = link("a", 4);
    assert_eq!(claim_link(&on_b, "b", "a", a_in), Ok(()));
    assert!(b_out_cancel.is_cancelled());
    assert_eq!(on_b.get("a").unwrap().dialer, "a");

    // A later dial by "b" loses on both nodes.
    let (late, late_cancel) = link("b", 5);
    assert_eq!(
        claim_link(&on_a, "a", "b", late),
        Err(LinkRefused::Duplicate)
    );
    assert!(!late_cancel.is_cancelled());
}

#[test]
fn a_redial_replaces_a_silent_link_and_a_live_one_is_kept() {
    // The peer dials again after its earlier link went quiet: half-open.
    let links = Links::new();
    let (old, old_cancel) = link_heard("peer", 1, 0);
    assert_eq!(claim_link(&links, "me", "peer", old), Ok(()));
    let (redial, _) = link("peer", 2);
    assert_eq!(claim_link(&links, "me", "peer", redial), Ok(()));
    assert!(old_cancel.is_cancelled());

    // The peer knows this node under two addresses: its earlier link is
    // alive and stays, on this side and on the dialer's.
    let (second_address, second_cancel) = link("peer", 3);
    assert_eq!(
        claim_link(&links, "me", "peer", second_address),
        Err(LinkRefused::Duplicate)
    );
    assert!(!second_cancel.is_cancelled());
    assert_eq!(links.get("peer").unwrap().session, 2);

    let other = Links::new();
    let (mine, _) = link("me", 1);
    assert_eq!(claim_link(&other, "me", "peer", mine), Ok(()));
    let (mine_again, _) = link("me", 2);
    assert_eq!(
        claim_link(&other, "me", "peer", mine_again),
        Err(LinkRefused::Duplicate)
    );
}

#[test]
fn self_links_are_refused_and_release_leaves_a_successor_alone() {
    let links = Links::new();
    let (to_self, _) = link("me", 1);
    assert_eq!(
        claim_link(&links, "me", "me", to_self),
        Err(LinkRefused::SelfLink)
    );

    // A silent first link, replaced by the peer's redial.
    let (first, _) = link_heard("peer", 1, 0);
    claim_link(&links, "me", "peer", first).unwrap();
    let (second, _) = link("peer", 2);
    claim_link(&links, "me", "peer", second).unwrap();
    release_link(&links, "peer", 1); // the replaced session cleaning up late
    assert_eq!(links.get("peer").unwrap().session, 2);
    release_link(&links, "peer", 2);
    assert!(links.get("peer").is_none());
}

#[test]
fn gossip_addresses_may_be_ips_or_dns_names() {
    for ok in [
        "10.0.0.2:9999",
        "[::1]:9999",
        "wse-1.wse.default.svc:9999",
        "node-2:7000",
    ] {
        assert!(super::manager::valid_peer_addr(ok), "{ok}");
    }
    for bad in [
        "10.0.0.2",
        "host:0",
        "http://x:1",
        "a b:1",
        "-bad:1",
        ":1",
        "x:99999",
    ] {
        assert!(!super::manager::valid_peer_addr(bad), "{bad}");
    }
}

#[test]
fn tls_server_names_handle_ipv6_and_dns_peers() {
    use rustls::pki_types::ServerName;
    assert!(matches!(
        super::peer::server_name_for("[::1]:9000"),
        Some(ServerName::IpAddress(_))
    ));
    assert!(matches!(
        super::peer::server_name_for("10.1.2.3:9000"),
        Some(ServerName::IpAddress(_))
    ));
    assert!(matches!(
        super::peer::server_name_for("wse-0.wse:9000"),
        Some(ServerName::DnsName(_))
    ));
    assert!(super::peer::server_name_for("no-port").is_none());
}

#[test]
fn peer_interest_routes_exact_topics_and_patterns_within_its_cap() {
    let mut interest = PeerInterest::default();
    assert!(interest.insert("chat.general".into(), 3));
    assert!(interest.insert("prices.*".into(), 3));
    assert!(
        interest.insert("chat.general".into(), 3),
        "a topic already held always fits"
    );
    assert!(interest.insert("orders".into(), 3));
    assert!(!interest.insert("one.too.many".into(), 3));
    assert_eq!(interest.len(), 3);

    assert!(interest.wants("chat.general"));
    assert!(interest.wants("prices.AAPL"));
    assert!(!interest.wants("chat.private"));

    interest.remove("prices.*");
    assert!(!interest.wants("prices.AAPL"));
    interest.remove("chat.general");
    assert!(!interest.wants("chat.general"));
    assert_eq!(interest.len(), 1);
}

#[test]
fn presence_rounds_carry_their_edges() {
    let mut buf = BytesMut::new();
    assert!(encode_presence_round(
        &mut buf,
        "{}",
        PRESENCE_ROUND_FIRST | PRESENCE_ROUND_LAST
    ));
    match decode_frame(buf).unwrap() {
        ClusterFrame::PresenceFull { entries, flags } => {
            assert_eq!(entries, "{}");
            assert_eq!(
                flags,
                PRESENCE_ROUND | PRESENCE_ROUND_FIRST | PRESENCE_ROUND_LAST
            );
        }
        other => panic!("Expected PresenceFull, got {other:?}"),
    }
}

/// The flags of every frame of the round `pm` sends now.
fn round_flags(pm: &crate::presence::PresenceManager) -> Vec<u8> {
    super::manager::presence_round(pm)
        .into_iter()
        .map(|frame| match decode_frame(BytesMut::from(&frame[..])) {
            Some(ClusterFrame::PresenceFull { flags, .. }) => flags,
            other => panic!("Expected PresenceFull, got {other:?}"),
        })
        .collect()
}

#[test]
fn a_presence_round_marks_its_first_and_last_frame() {
    const ONLY: u8 = PRESENCE_ROUND | PRESENCE_ROUND_FIRST | PRESENCE_ROUND_LAST;
    let pm = crate::presence::PresenceManager::new(65_536, 0);
    // Nothing present is a round too: it clears what the peer held.
    assert_eq!(round_flags(&pm), [ONLY]);

    let big = serde_json::json!({ "bio": "x".repeat(60_000) });
    for i in 0..40 {
        let conn = format!("conn{i}");
        pm.register_connection(&conn, &format!("user{i}"));
        pm.track(&conn, "room", &big);
    }
    let flags = round_flags(&pm);
    assert!(flags.len() > 1, "the state spans several frames");
    assert_eq!(flags[0], PRESENCE_ROUND | PRESENCE_ROUND_FIRST);
    assert_eq!(*flags.last().unwrap(), PRESENCE_ROUND | PRESENCE_ROUND_LAST);
    assert!(
        flags[1..flags.len() - 1]
            .iter()
            .all(|f| *f == PRESENCE_ROUND)
    );
}

#[test]
fn snapshots_are_queued_whole_or_not_at_all() {
    let (tx, mut rx) = mpsc::channel::<Bytes>(3);
    tx.try_send(Bytes::from_static(b"a")).unwrap();
    tx.try_send(Bytes::from_static(b"b")).unwrap();
    let frames = [Bytes::from_static(b"1"), Bytes::from_static(b"2")];
    assert!(!super::manager::queue_whole(&tx, &frames));
    assert!(super::manager::queue_whole(&tx, &frames[..1]));
    let queued: Vec<Bytes> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert_eq!(queued, [&b"a"[..], &b"b"[..], &b"1"[..]]);
}

/// A peer that stops reading: once the link is cancelled (its heartbeat ran
/// out), the writer stops at once instead of waiting on a full socket for
/// as long as the peer keeps it open.
#[tokio::test]
async fn a_cancelled_link_stops_writing_to_a_peer_that_stopped_reading() {
    let (ours, _theirs) = tokio::io::duplex(1024);
    let (tx, rx) = mpsc::channel::<Bytes>(16);
    let cancel = tokio_util::sync::CancellationToken::new();
    let writer = tokio::spawn(super::peer::peer_writer(
        rx,
        ours,
        cancel.clone(),
        tokio_util::sync::CancellationToken::new(),
        Arc::new(ClusterMetrics::new()),
    ));
    for _ in 0..4 {
        tx.send(Bytes::from(vec![0u8; 256 * 1024])).await.unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!writer.is_finished(), "the writer waits on the full socket");
    cancel.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(1), writer)
        .await
        .expect("the writer stops once the link is cancelled")
        .unwrap();
}

/// A node that leaves: its link's writer finishes the batch it was cut
/// short in, writes what was queued after it (the SHUTDOWN last, on a
/// stop), then closes its side, so the peer reads every frame whole.
#[tokio::test]
async fn a_link_of_a_node_that_leaves_writes_its_frames_whole_then_closes() {
    use tokio::io::AsyncReadExt;
    let (ours, mut theirs) = tokio::io::duplex(1024);
    let (tx, rx) = mpsc::channel::<Bytes>(16);
    let cancel = tokio_util::sync::CancellationToken::new();
    let leaving = tokio_util::sync::CancellationToken::new();
    let writer = tokio::spawn(super::peer::peer_writer(
        rx,
        ours,
        cancel.clone(),
        leaving.clone(),
        Arc::new(ClusterMetrics::new()),
    ));
    let frames: Vec<Bytes> = (0u8..4).map(|n| Bytes::from(vec![n; 3000])).collect();
    // More than the socket takes: the writer waits in the middle of it.
    tx.send(frames[0].clone()).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    for frame in &frames[1..] {
        tx.send(frame.clone()).await.unwrap();
    }
    leaving.cancel();
    cancel.cancel();

    let mut wire = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        theirs.read_to_end(&mut wire),
    )
    .await
    .expect("the link closes")
    .unwrap();
    let mut read = &wire[..];
    for (n, frame) in frames.iter().enumerate() {
        assert!(read.len() >= 4, "frame {n} missing");
        let len = u32::from_be_bytes(read[..4].try_into().unwrap()) as usize;
        assert!(read.len() >= 4 + len, "frame {n} cut short");
        assert_eq!(&read[4..4 + len], &frame[..], "frame {n}");
        read = &read[4 + len..];
    }
    assert!(read.is_empty(), "nothing after the frames");
    writer.await.unwrap();
}
