//! Msgpack for the engine: depth-guarded decoding of what clients send, and
//! the conversions between msgpack and JSON values. No Python here; the
//! Python-facing msgpack functions live in `compression`.

/// Maximum msgpack nesting depth accepted on inbound decode.
///
/// `rmpv::decode::read_value` is a recursive-descent decoder with NO depth
/// limit, so a hostile deeply-nested payload (e.g. ~1M repeated `0x91` fixarray
/// markers packed into a 1 MB frame) overflows the thread stack and aborts the
/// whole process. Such frames are rejected BEFORE decoding. 64 is far beyond
/// any legitimate event shape and is the same order of magnitude as
/// serde_json's built-in recursion budget, which already protects the JSON path.
pub(crate) const MAX_MSGPACK_DEPTH: usize = 64;

/// Read an `n`-byte big-endian unsigned length/count at `*pos`, advancing `pos`.
fn read_be_uint(data: &[u8], pos: &mut usize, n: usize) -> Result<usize, ()> {
    let mut v: usize = 0;
    for _ in 0..n {
        let b = *data.get(*pos).ok_or(())?;
        *pos += 1;
        v = (v << 8) | b as usize;
    }
    Ok(v)
}

/// Open a container level, rejecting if it would exceed `max_depth`.
fn open_container(remaining: &mut Vec<u64>, children: u64, max_depth: usize) -> Result<(), ()> {
    if remaining.len() >= max_depth {
        return Err(());
    }
    remaining.push(children);
    Ok(())
}

/// Structurally validate that `data` begins with a single msgpack value whose
/// nesting depth does not exceed `max_depth`, WITHOUT building the value tree
/// (iterative, no recursion, no decoded allocation, O(len)). Returns `Err` on a
/// depth violation, a truncated/invalid frame, or the reserved `0xc1` marker.
///
/// Run before `rmpv::decode::read_value`, whose recursive decoder would
/// otherwise overflow the stack on a hostile deeply-nested frame.
pub(crate) fn check_msgpack_depth(data: &[u8], max_depth: usize) -> Result<(), ()> {
    // Each stack entry is the count of child VALUES still to be read at that
    // open container level; current depth == stack length.
    let mut remaining: Vec<u64> = Vec::new();
    let mut pos: usize = 0;

    loop {
        // Close every container whose children are all consumed.
        while matches!(remaining.last(), Some(&0)) {
            remaining.pop();
        }
        // One complete top-level value has been read and all containers closed.
        if remaining.is_empty() && pos > 0 {
            return Ok(());
        }
        // Count the value we are about to read against its parent container.
        if let Some(top) = remaining.last_mut() {
            *top -= 1;
        }

        let marker = *data.get(pos).ok_or(())?;
        pos += 1;

        match marker {
            // fixint (pos/neg), nil, bool: no payload, no children.
            0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => {}
            0xc1 => return Err(()), // reserved -- never a valid value
            0x80..=0x8f => open_container(&mut remaining, (marker & 0x0f) as u64 * 2, max_depth)?, // fixmap
            0x90..=0x9f => open_container(&mut remaining, (marker & 0x0f) as u64, max_depth)?, // fixarray
            0xa0..=0xbf => pos = pos.checked_add((marker & 0x1f) as usize).ok_or(())?, // fixstr
            0xc4 | 0xd9 => {
                let l = read_be_uint(data, &mut pos, 1)?; // bin8 / str8
                pos = pos.checked_add(l).ok_or(())?;
            }
            0xc5 | 0xda => {
                let l = read_be_uint(data, &mut pos, 2)?; // bin16 / str16
                pos = pos.checked_add(l).ok_or(())?;
            }
            0xc6 | 0xdb => {
                let l = read_be_uint(data, &mut pos, 4)?; // bin32 / str32
                pos = pos.checked_add(l).ok_or(())?;
            }
            0xc7 => {
                let l = read_be_uint(data, &mut pos, 1)?; // ext8 (+1 type byte)
                pos = pos.checked_add(l + 1).ok_or(())?;
            }
            0xc8 => {
                let l = read_be_uint(data, &mut pos, 2)?; // ext16
                pos = pos.checked_add(l + 1).ok_or(())?;
            }
            0xc9 => {
                let l = read_be_uint(data, &mut pos, 4)?; // ext32
                pos = pos.checked_add(l + 1).ok_or(())?;
            }
            0xca => pos = pos.checked_add(4).ok_or(())?, // float32
            0xcb => pos = pos.checked_add(8).ok_or(())?, // float64
            0xcc | 0xd0 => pos = pos.checked_add(1).ok_or(())?, // uint8 / int8
            0xcd | 0xd1 => pos = pos.checked_add(2).ok_or(())?, // uint16 / int16
            0xce | 0xd2 => pos = pos.checked_add(4).ok_or(())?, // uint32 / int32
            0xcf | 0xd3 => pos = pos.checked_add(8).ok_or(())?, // uint64 / int64
            0xd4 => pos = pos.checked_add(1 + 1).ok_or(())?, // fixext1
            0xd5 => pos = pos.checked_add(1 + 2).ok_or(())?, // fixext2
            0xd6 => pos = pos.checked_add(1 + 4).ok_or(())?, // fixext4
            0xd7 => pos = pos.checked_add(1 + 8).ok_or(())?, // fixext8
            0xd8 => pos = pos.checked_add(1 + 16).ok_or(())?, // fixext16
            0xdc => {
                let n = read_be_uint(data, &mut pos, 2)? as u64; // array16
                open_container(&mut remaining, n, max_depth)?;
            }
            0xdd => {
                let n = read_be_uint(data, &mut pos, 4)? as u64; // array32
                open_container(&mut remaining, n, max_depth)?;
            }
            0xde => {
                let n = read_be_uint(data, &mut pos, 2)? as u64; // map16
                open_container(&mut remaining, n * 2, max_depth)?;
            }
            0xdf => {
                let n = read_be_uint(data, &mut pos, 4)? as u64; // map32
                open_container(&mut remaining, n * 2, max_depth)?;
            }
        }

        // A scalar payload skip (str/bin/ext/fixstr/fixint-width) advances `pos`
        // past bytes it never indexed; reject here if that ran off the end so a
        // truncated value is an Err rather than a false "valid" pass.
        if pos > data.len() {
            return Err(());
        }
    }
}

/// Depth-guarded msgpack decode: reject frames deeper than `MAX_MSGPACK_DEPTH`
/// (which would overflow rmpv's recursive decoder) before decoding. `Err(())`
/// means "not safely decodable" -- callers fall back to raw bytes / JSON.
pub(crate) fn decode_msgpack_guarded(data: &[u8]) -> Result<rmpv::Value, ()> {
    check_msgpack_depth(data, MAX_MSGPACK_DEPTH)?;
    let mut rest = data;
    let value = rmpv::decode::read_value(&mut rest).map_err(|_| ())?;
    // One msgpack value must span the whole buffer. JSON is not msgpack even
    // though its first byte usually decodes: '{' (0x7b) is the integer 123.
    if !rest.is_empty() {
        return Err(());
    }
    Ok(value)
}

/// Convert rmpv::Value -> serde_json::Value: inbound msgpack as the JSON
/// value the rest of the server works with.
pub fn rmpv_to_serde_json(val: &rmpv::Value) -> serde_json::Value {
    match val {
        rmpv::Value::Nil => serde_json::Value::Null,
        rmpv::Value::Boolean(b) => serde_json::Value::Bool(*b),
        rmpv::Value::Integer(i) => {
            if let Some(v) = i.as_i64() {
                serde_json::Value::Number(serde_json::Number::from(v))
            } else if let Some(v) = i.as_u64() {
                serde_json::Value::Number(serde_json::Number::from(v))
            } else {
                serde_json::Value::Number(serde_json::Number::from(0))
            }
        }
        rmpv::Value::F32(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        rmpv::Value::F64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        rmpv::Value::String(s) => serde_json::Value::String(s.as_str().unwrap_or("").to_string()),
        rmpv::Value::Binary(b) => {
            // Encode binary as base64 or hex in JSON context
            serde_json::Value::String(hex::encode(b))
        }
        rmpv::Value::Array(arr) => {
            let items: Vec<serde_json::Value> = arr.iter().map(rmpv_to_serde_json).collect();
            serde_json::Value::Array(items)
        }
        rmpv::Value::Map(pairs) => {
            let mut map = serde_json::Map::new();
            for (k, v) in pairs {
                let key = match k {
                    rmpv::Value::String(s) => s.as_str().unwrap_or("").to_string(),
                    _ => format!("{}", k),
                };
                map.insert(key, rmpv_to_serde_json(v));
            }
            serde_json::Value::Object(map)
        }
        rmpv::Value::Ext(_, data) => serde_json::Value::String(hex::encode(data)),
    }
}

/// A float as msgpack: 32-bit (5 bytes) when that holds it exactly, else
/// 64-bit (9 bytes). Short decimals such as 64000.5 or 0.25 would otherwise
/// take more room than their JSON text; readers widen it back unchanged.
pub(crate) fn float_value(f: f64) -> rmpv::Value {
    let narrow = f as f32;
    if f64::from(narrow) == f {
        rmpv::Value::F32(narrow)
    } else {
        rmpv::Value::F64(f)
    }
}

/// Convert serde_json::Value to rmpv::Value for msgpack serialization: what
/// goes to a connection that asked for msgpack.
pub fn serde_json_to_rmpv(val: &serde_json::Value) -> rmpv::Value {
    match val {
        serde_json::Value::Null => rmpv::Value::Nil,
        serde_json::Value::Bool(b) => rmpv::Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rmpv::Value::Integer(rmpv::Integer::from(i))
            } else if let Some(u) = n.as_u64() {
                rmpv::Value::Integer(rmpv::Integer::from(u))
            } else if let Some(f) = n.as_f64() {
                float_value(f)
            } else {
                rmpv::Value::Nil
            }
        }
        serde_json::Value::String(s) => rmpv::Value::String(s.clone().into()),
        serde_json::Value::Array(arr) => {
            rmpv::Value::Array(arr.iter().map(serde_json_to_rmpv).collect())
        }
        serde_json::Value::Object(map) => {
            let pairs: Vec<(rmpv::Value, rmpv::Value)> = map
                .iter()
                .map(|(k, v)| (rmpv::Value::String(k.clone().into()), serde_json_to_rmpv(v)))
                .collect();
            rmpv::Value::Map(pairs)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Encode a chain of `n` nested single-element fixarrays terminated by a nil:
    // 0x91 0x91 ... 0x91 0xc0  -> nesting depth n. This is the stack-overflow bomb.
    fn nested_arrays(n: usize) -> Vec<u8> {
        let mut v = vec![0x91u8; n];
        v.push(0xc0); // innermost element: nil
        v
    }

    #[test]
    fn depth_scan_accepts_shallow_values() {
        // scalars
        assert!(check_msgpack_depth(&[0x00], MAX_MSGPACK_DEPTH).is_ok()); // positive fixint
        assert!(check_msgpack_depth(&[0xc0], MAX_MSGPACK_DEPTH).is_ok()); // nil
        assert!(check_msgpack_depth(&[0xc3], MAX_MSGPACK_DEPTH).is_ok()); // true
        // fixstr "hi"
        assert!(check_msgpack_depth(&[0xa2, b'h', b'i'], MAX_MSGPACK_DEPTH).is_ok());
        // empty array / empty map
        assert!(check_msgpack_depth(&[0x90], MAX_MSGPACK_DEPTH).is_ok());
        assert!(check_msgpack_depth(&[0x80], MAX_MSGPACK_DEPTH).is_ok());
        // {"k": [1, 2]} == fixmap1 { fixstr "k": fixarray2 [1,2] }
        let buf = [0x81, 0xa1, b'k', 0x92, 0x01, 0x02];
        assert!(check_msgpack_depth(&buf, MAX_MSGPACK_DEPTH).is_ok());
    }

    #[test]
    fn depth_scan_accepts_exactly_max_depth_and_rejects_deeper() {
        // Exactly MAX_MSGPACK_DEPTH nested arrays must pass.
        assert!(check_msgpack_depth(&nested_arrays(MAX_MSGPACK_DEPTH), MAX_MSGPACK_DEPTH).is_ok());
        // One deeper must be rejected.
        assert!(
            check_msgpack_depth(&nested_arrays(MAX_MSGPACK_DEPTH + 1), MAX_MSGPACK_DEPTH).is_err()
        );
    }

    #[test]
    fn depth_scan_rejects_pathological_bomb_quickly() {
        // ~100k nested arrays (would blow rmpv's recursive decoder stack). The
        // scan must reject without recursing and without reading the whole input.
        let bomb = vec![0x91u8; 100_000];
        assert!(check_msgpack_depth(&bomb, MAX_MSGPACK_DEPTH).is_err());
    }

    #[test]
    fn depth_scan_rejects_truncated_and_reserved() {
        // Truncated: fixarray claims 1 child but no bytes follow.
        assert!(check_msgpack_depth(&[0x91], MAX_MSGPACK_DEPTH).is_err());
        // Truncated str: fixstr len 5 but no payload.
        assert!(check_msgpack_depth(&[0xa5], MAX_MSGPACK_DEPTH).is_err());
        // Reserved marker 0xc1 is never a valid value.
        assert!(check_msgpack_depth(&[0xc1], MAX_MSGPACK_DEPTH).is_err());
        // Empty input.
        assert!(check_msgpack_depth(&[], MAX_MSGPACK_DEPTH).is_err());
    }

    #[test]
    fn floats_take_32_bits_only_when_that_is_exact() {
        for f in [64000.5, 0.25, -0.0, 1.0, 1e30_f32 as f64] {
            assert_eq!(float_value(f), rmpv::Value::F32(f as f32), "{f}");
        }
        for f in [187.42, 0.1, 1e300, f64::from(f32::MAX) * 2.0] {
            assert_eq!(float_value(f), rmpv::Value::F64(f), "{f}");
        }
    }

    #[test]
    fn guarded_decode_matches_rmpv_for_valid_and_rejects_bomb() {
        // Valid shallow value decodes identically to raw rmpv.
        let buf = [0x92, 0x01, 0x02]; // [1, 2]
        let guarded = decode_msgpack_guarded(&buf).expect("valid msgpack");
        let raw = rmpv::decode::read_value(&mut &buf[..]).unwrap();
        assert_eq!(guarded, raw);
        // Bomb is rejected before rmpv ever recurses.
        assert!(decode_msgpack_guarded(&nested_arrays(MAX_MSGPACK_DEPTH + 5)).is_err());
    }

    mod properties {
        use super::super::*;
        use proptest::prelude::*;

        fn rmpv_value() -> impl Strategy<Value = rmpv::Value> {
            let leaf = prop_oneof![
                Just(rmpv::Value::Nil),
                any::<bool>().prop_map(rmpv::Value::Boolean),
                any::<i64>().prop_map(|i| rmpv::Value::Integer(i.into())),
                any::<u64>().prop_map(|u| rmpv::Value::Integer(u.into())),
                any::<f64>()
                    .prop_filter("NaN never equals itself", |f| !f.is_nan())
                    .prop_map(rmpv::Value::F64),
                ".{0,24}".prop_map(|s| rmpv::Value::String(s.into())),
                proptest::collection::vec(any::<u8>(), 0..24).prop_map(rmpv::Value::Binary),
            ];
            leaf.prop_recursive(6, 64, 6, |inner| {
                prop_oneof![
                    proptest::collection::vec(inner.clone(), 0..6).prop_map(rmpv::Value::Array),
                    proptest::collection::vec((inner.clone(), inner), 0..6)
                        .prop_map(rmpv::Value::Map),
                ]
            })
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(2000))]

            #[test]
            fn arbitrary_bytes_never_panic(data in proptest::collection::vec(any::<u8>(), 0..2048)) {
                let _ = check_msgpack_depth(&data, MAX_MSGPACK_DEPTH);
                let _ = decode_msgpack_guarded(&data);
            }

            #[test]
            fn msgpack_round_trips(value in rmpv_value()) {
                let mut buf = Vec::new();
                rmpv::encode::write_value(&mut buf, &value).unwrap();
                prop_assert_eq!(decode_msgpack_guarded(&buf), Ok(value));
            }

            #[test]
            fn trailing_bytes_are_rejected(value in rmpv_value(), extra in proptest::collection::vec(any::<u8>(), 1..16)) {
                let mut buf = Vec::new();
                rmpv::encode::write_value(&mut buf, &value).unwrap();
                buf.extend_from_slice(&extra);
                prop_assert!(decode_msgpack_guarded(&buf).is_err());
            }
        }
    }
}
