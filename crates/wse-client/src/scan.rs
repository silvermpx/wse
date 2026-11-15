//! The fields of a JSON message found by scanning its bytes: the type, the
//! topic and its position, the ids, and the payload's text as it is. What
//! the scan does not take on (an escaped key, anything malformed) goes to
//! the full parser instead, which reads it the same way.

use std::borrow::Cow;

use crate::decode::{Envelope, Field};
use crate::message::Payload;

/// Nesting deeper than this goes to the full parser.
const MAX_DEPTH: u32 = 64;

/// The message's fields; `None` when the full parser must read it.
pub(crate) fn envelope(text: &str) -> Option<Envelope<'_>> {
    let b = text.as_bytes();
    let mut i = ws(b, 0);
    if b.get(i) != Some(&b'{') {
        return None;
    }
    let mut env = Envelope::default();
    i = ws(b, i + 1);
    if b.get(i) == Some(&b'}') {
        return (ws(b, i + 1) == b.len()).then_some(env);
    }
    loop {
        if b.get(i) != Some(&b'"') {
            return None;
        }
        let start = i + 1;
        let end = start + memchr::memchr2(b'"', b'\\', &b[start..])?;
        if b[end] == b'\\' {
            return None;
        }
        let key = &text[start..end];
        i = ws(b, end + 1);
        if b.get(i) != Some(&b':') {
            return None;
        }
        i = ws(b, i + 1);
        match Field::of(key) {
            Field::P => {
                let start = i;
                i = value(b, i)?;
                let raw = &text[start..i];
                env.p = (raw != "null").then_some(Payload::Json(raw));
            }
            Field::O => (env.o, i) = uint(text, i)?,
            Field::Pri => (env.pri, i) = uint(text, i)?,
            Field::Other => i = value(b, i)?,
            field => {
                let (s, next) = string(text, i)?;
                i = next;
                match field {
                    Field::T => env.t = s,
                    Field::C => env.c = s,
                    Field::Tp => env.tp = s,
                    Field::E => env.e = s,
                    Field::Id => env.id = s,
                    Field::Cid => env.cid = s,
                    Field::Ts => env.ts = s,
                    Field::Sb => env.sb = s,
                    Field::Sig => env.sig = s,
                    Field::Kid => env.kid = s,
                    Field::N => env.n = s,
                    Field::P | Field::O | Field::Pri | Field::Other => unreachable!(),
                }
            }
        }
        i = ws(b, i);
        match b.get(i) {
            Some(b',') => i = ws(b, i + 1),
            Some(b'}') => return (ws(b, i + 1) == b.len()).then_some(env),
            _ => return None,
        }
    }
}

fn ws(b: &[u8], mut i: usize) -> usize {
    while let Some(b' ' | b'\n' | b'\r' | b'\t') = b.get(i) {
        i += 1;
    }
    i
}

/// A string value (`None` for a value of another type, stepped over), and
/// where the scan goes on.
#[allow(clippy::type_complexity)]
fn string(text: &str, i: usize) -> Option<(Option<Cow<'_, str>>, usize)> {
    let b = text.as_bytes();
    if b.get(i) != Some(&b'"') {
        return Some((None, value(b, i)?));
    }
    let start = i + 1;
    let end = start + memchr::memchr2(b'"', b'\\', &b[start..])?;
    if b[end] == b'"' {
        return Some((Some(Cow::Borrowed(&text[start..end])), end + 1));
    }
    // Escapes: the full parser unescapes it.
    let end = string_end(b, i)?;
    let owned: String = serde_json::from_str(&text[i..end]).ok()?;
    Some((Some(Cow::Owned(owned)), end))
}

/// A non-negative integer that fits 64 bits (`None` for any other value,
/// stepped over), and where the scan goes on.
fn uint(text: &str, i: usize) -> Option<(Option<u64>, usize)> {
    let b = text.as_bytes();
    let digits = b[i..].iter().take_while(|c| c.is_ascii_digit()).count();
    let end = i + digits;
    let whole = digits > 0 && !matches!(b.get(end), Some(b'.' | b'e' | b'E'));
    if !whole {
        return Some((None, value(b, i)?));
    }
    // Leading zeros are not JSON: the full parser refuses them.
    if digits > 1 && b[i] == b'0' {
        return None;
    }
    Some((text[i..end].parse().ok(), end))
}

/// Where the string starting at `i` (its opening quote) ends, past its
/// closing quote.
fn string_end(b: &[u8], i: usize) -> Option<usize> {
    let mut at = i + 1;
    loop {
        // An escape as the frame's last byte leaves `at` past the end.
        at += memchr::memchr2(b'"', b'\\', b.get(at..)?)?;
        match b[at] {
            b'"' => return Some(at + 1),
            _ => at += 2,
        }
    }
}

/// Where the value starting at `i` ends. Objects and arrays are stepped
/// over by their brackets, each closed by its own kind.
fn value(b: &[u8], i: usize) -> Option<usize> {
    match *b.get(i)? {
        b'"' => string_end(b, i),
        b'{' | b'[' => {
            // The open brackets, innermost lowest: 1 for an object.
            let mut stack: u64 = u64::from(b[i] == b'{');
            let mut depth = 1u32;
            let mut at = i + 1;
            loop {
                match *b.get(at)? {
                    b'"' => {
                        at = string_end(b, at)?;
                        continue;
                    }
                    open @ (b'{' | b'[') => {
                        if depth == MAX_DEPTH {
                            return None;
                        }
                        stack = (stack << 1) | u64::from(open == b'{');
                        depth += 1;
                    }
                    close @ (b'}' | b']') => {
                        if (stack & 1 == 1) != (close == b'}') {
                            return None;
                        }
                        stack >>= 1;
                        depth -= 1;
                        if depth == 0 {
                            return Some(at + 1);
                        }
                    }
                    _ => {}
                }
                at += 1;
            }
        }
        b't' => literal(b, i, b"true"),
        b'f' => literal(b, i, b"false"),
        b'n' => literal(b, i, b"null"),
        b'-' | b'0'..=b'9' => {
            let len = b[i..]
                .iter()
                .take_while(|c| matches!(c, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'))
                .count();
            Some(i + len)
        }
        _ => None,
    }
}

fn literal(b: &[u8], i: usize, word: &[u8]) -> Option<usize> {
    b[i..].starts_with(word).then_some(i + word.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::json_envelope_full;

    /// The fields both readers found, as owned text.
    fn fields(env: &Envelope<'_>) -> Vec<Option<String>> {
        let text = |s: &Option<Cow<'_, str>>| s.as_deref().map(str::to_owned);
        vec![
            text(&env.t),
            text(&env.c),
            text(&env.tp),
            text(&env.e),
            text(&env.id),
            text(&env.cid),
            text(&env.ts),
            text(&env.sb),
            text(&env.sig),
            text(&env.kid),
            text(&env.n),
            env.o.map(|o| o.to_string()),
            env.pri.map(|p| p.to_string()),
            match env.p {
                Some(Payload::Json(p)) => Some(p.to_owned()),
                Some(Payload::Msgpack(_)) => Some("msgpack".to_owned()),
                Some(Payload::None) | None => None,
            },
        ]
    }

    #[test]
    fn a_server_message_is_scanned() {
        let text = r#"{"tp":"prices","e":"0000abcd","o":42,"c":"U","t":"tick","p":{"px":1.5,"s":"a\"}b"},"v":1}"#;
        let env = envelope(text).unwrap();
        assert_eq!(env.t.as_deref(), Some("tick"));
        assert_eq!(env.o, Some(42));
        assert!(matches!(
            env.p,
            Some(Payload::Json(r#"{"px":1.5,"s":"a\"}b"}"#))
        ));
        assert!(matches!(env.t, Some(Cow::Borrowed(_))));
    }

    #[test]
    fn what_the_scan_does_not_take_on_goes_to_the_full_parser() {
        for text in [
            r#"{"\u0074":"x"}"#,
            r#"{"t":"x"} trailing"#,
            r#"{"t":"x","p":{"a":[1}]}"#,
            r#"{"t":"x","o":007}"#,
            r#"{"t":"x" "p":1}"#,
            r#"["t"]"#,
            r#"{"t":tru}"#,
        ] {
            assert!(envelope(text).is_none(), "{text}");
        }
    }

    /// Every cut-short form of a message, such as a frame ending in the
    /// middle of an escape, is refused, never a panic.
    #[test]
    fn a_frame_cut_anywhere_is_refused() {
        let whole = r#"{"t":"a\"b","p":{"s":"x\\y","u":"é"},"o":12,"tp":"q"}"#;
        for end in (0..whole.len()).filter(|&end| whole.is_char_boundary(end)) {
            assert!(envelope(&whole[..end]).is_none(), "{}", &whole[..end]);
        }
        assert!(envelope(r#"{"t":"\"#).is_none());
        assert!(envelope(r#"{"p":{"a":"\"#).is_none());
    }

    /// Random messages read by both readers: the same fields, or the scan
    /// leaves it to the full parser.
    #[test]
    fn the_scan_reads_what_the_full_parser_reads() {
        let mut rng = fastrand::Rng::with_seed(7);
        let strings = [
            "tick",
            "",
            "a\\\"b",
            "\\u00e9t\\u00e9",
            "x\\\\",
            "{\\\"t\\\":1}",
            "café",
            "]}",
        ];
        let keys = [
            "t", "p", "c", "tp", "e", "o", "id", "cid", "ts", "pri", "sb", "sig", "kid", "n",
            "type", "payload", "v", "seq", "x",
        ];
        let value = |rng: &mut fastrand::Rng, depth: u32| -> String {
            fn go(rng: &mut fastrand::Rng, depth: u32, strings: &[&str]) -> String {
                match rng.u8(0..9) {
                    0 => format!("\"{}\"", strings[rng.usize(..strings.len())]),
                    1 => rng.u64(..).to_string(),
                    2 => format!("-{}", rng.u32(..)),
                    3 => format!("{}.{}e{}", rng.u32(..), rng.u8(..), rng.i8(..)),
                    4 => ["true", "false", "null"][rng.usize(..3)].to_owned(),
                    5 if depth < 4 => {
                        let n = rng.usize(0..4);
                        let items: Vec<String> =
                            (0..n).map(|_| go(rng, depth + 1, strings)).collect();
                        format!("[{}]", items.join(","))
                    }
                    6 if depth < 4 => {
                        let n = rng.usize(0..4);
                        let items: Vec<String> = (0..n)
                            .map(|_| {
                                format!(
                                    "\"{}\":{}",
                                    strings[rng.usize(..strings.len())],
                                    go(rng, depth + 1, strings)
                                )
                            })
                            .collect();
                        format!("{{{}}}", items.join(","))
                    }
                    _ => format!("\"{}\"", strings[rng.usize(..strings.len())]),
                }
            }
            go(rng, depth, &strings)
        };
        let mut scanned = 0;
        for _ in 0..20_000 {
            let n = rng.usize(0..8);
            let members: Vec<String> = (0..n)
                .map(|_| {
                    let key = keys[rng.usize(..keys.len())];
                    let pad = [" ", "", "\n"][rng.usize(..3)];
                    format!("{pad}\"{key}\"{pad}:{pad}{}", value(&mut rng, 0))
                })
                .collect();
            let text = format!("{{{}}}", members.join(","));
            let full = json_envelope_full(&text).ok();
            match envelope(&text) {
                Some(env) => {
                    scanned += 1;
                    let full = full.unwrap_or_else(|| panic!("scanned, refused in full: {text}"));
                    assert_eq!(fields(&env), fields(&full), "{text}");
                }
                None => assert!(
                    full.is_none() || text.contains("\\u0074"),
                    "left to the full parser: {text}"
                ),
            }
        }
        assert!(scanned > 15_000, "{scanned}");
    }
}
