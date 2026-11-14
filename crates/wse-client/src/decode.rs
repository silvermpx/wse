//! Reading the server's frames in place: JSON (text or binary frames), `C:`
//! zlib, `M:` msgpack, batches of messages.

use std::borrow::Cow;
use std::fmt;

use bytes::Bytes;
use flate2::{Decompress, FlushDecompress, Status};
use serde::de::{self, Deserialize, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;

use crate::message::{Message, Payload, Position};

/// Nesting deeper than this in a msgpack frame is refused, as on the
/// server: a crafted frame cannot overflow the stack.
const MAX_DEPTH: u32 = 64;

/// The inflate buffer keeps at most this much between frames.
const KEEP_INFLATED: usize = 1 << 20;

/// Why a frame was dropped.
#[derive(Debug)]
pub(crate) enum Dropped {
    TooBig,
    Corrupt(&'static str),
    Json(serde_json::Error),
}

impl fmt::Display for Dropped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooBig => f.write_str("over the size limit"),
            Self::Corrupt(what) => write!(f, "corrupt {what}"),
            Self::Json(e) => write!(f, "not JSON: {e}"),
        }
    }
}

/// What a frame holds.
pub(crate) enum Body<'a> {
    /// JSON text.
    Json(&'a str),
    /// A msgpack value.
    Msgpack(&'a [u8]),
    /// Sealed for an end-to-end session (`E:`).
    Sealed,
    /// Bytes that are none of these: the application's own.
    Binary,
}

/// Reads frames; keeps its inflate state and buffer between them.
pub(crate) struct Decoder {
    max_size: usize,
    inflater: Decompress,
    inflated: Vec<u8>,
}

impl Decoder {
    pub(crate) fn new(max_size: usize) -> Self {
        Self {
            max_size,
            inflater: Decompress::new(true),
            inflated: Vec::new(),
        }
    }

    /// What a binary frame holds. `C:` frames are inflated into the
    /// decoder's buffer, which the answer borrows.
    pub(crate) fn binary<'a>(&'a mut self, data: &'a [u8]) -> Result<Body<'a>, Dropped> {
        match data {
            [b'{', ..] => text(data).map(Body::Json),
            [b'C', b':', rest @ ..] => {
                self.inflate(rest)?;
                text(&self.inflated).map(Body::Json)
            }
            [b'M', b':', rest @ ..] => Ok(Body::Msgpack(rest)),
            [b'E', b':', ..] => Ok(Body::Sealed),
            // A zlib stream without the prefix.
            [0x78, 0x01 | 0x5e | 0x9c | 0xda, ..] => {
                self.inflate(data)?;
                text(&self.inflated).map(Body::Json)
            }
            [0x80..=0x8f | 0xde | 0xdf, ..] => Ok(Body::Msgpack(data)),
            _ => match std::str::from_utf8(data) {
                Ok(json) if json.trim_start().starts_with('{') => Ok(Body::Json(json)),
                _ => Ok(Body::Binary),
            },
        }
    }

    /// Inflates `data` into the buffer, never past the size limit.
    fn inflate(&mut self, mut data: &[u8]) -> Result<(), Dropped> {
        if self.inflated.capacity() > KEEP_INFLATED {
            self.inflated = Vec::new();
        }
        self.inflated.clear();
        self.inflater.reset(true);
        loop {
            if self.inflated.len() == self.inflated.capacity() {
                if self.inflated.len() > self.max_size {
                    return Err(Dropped::TooBig);
                }
                let room = self.inflated.len().max(4096);
                self.inflated
                    .reserve(room.min(self.max_size + 1 - self.inflated.len()));
            }
            let (taken, made) = (self.inflater.total_in(), self.inflated.len());
            let status = self
                .inflater
                .decompress_vec(data, &mut self.inflated, FlushDecompress::Finish)
                .map_err(|_| Dropped::Corrupt("zlib stream"))?;
            let used = (self.inflater.total_in() - taken) as usize;
            data = &data[used..];
            if self.inflated.len() > self.max_size {
                return Err(Dropped::TooBig);
            }
            let stuck = used == 0
                && self.inflated.len() == made
                && self.inflated.len() < self.inflated.capacity();
            match status {
                Status::StreamEnd => return Ok(()),
                // No progress with room to write: the stream is cut short.
                _ if stuck => return Err(Dropped::Corrupt("zlib stream")),
                _ => {}
            }
        }
    }
}

fn text(data: &[u8]) -> Result<&str, Dropped> {
    std::str::from_utf8(data).map_err(|_| Dropped::Corrupt("UTF-8"))
}

/// The fields of a message the client reads; the rest is skipped.
#[derive(Default)]
pub(crate) struct Envelope<'a> {
    pub(crate) t: Option<Cow<'a, str>>,
    pub(crate) p: Option<Payload<'a>>,
    pub(crate) c: Option<Cow<'a, str>>,
    pub(crate) tp: Option<Cow<'a, str>>,
    pub(crate) e: Option<Cow<'a, str>>,
    pub(crate) o: Option<u64>,
    pub(crate) id: Option<Cow<'a, str>>,
    pub(crate) cid: Option<Cow<'a, str>>,
    pub(crate) ts: Option<Cow<'a, str>>,
    pub(crate) pri: Option<u64>,
    /// A signed message: the body as signed, its signature, the key's id.
    pub(crate) sb: Option<Cow<'a, str>>,
    pub(crate) sig: Option<Cow<'a, str>>,
    pub(crate) kid: Option<Cow<'a, str>>,
    /// A signed body's nonce.
    pub(crate) n: Option<Cow<'a, str>>,
}

impl<'a> Envelope<'a> {
    /// The message it describes; the frame lets an owned copy share it.
    pub(crate) fn into_message(self, frame: Option<&'a Bytes>) -> Message<'a> {
        let position = match (&self.tp, &self.e, self.o) {
            (Some(_), Some(epoch), Some(offset)) => {
                epoch_of(epoch).map(|epoch| Position { epoch, offset })
            }
            _ => None,
        };
        Message {
            kind: self.t.unwrap_or(Cow::Borrowed("unknown")),
            category: self.c,
            payload: self.p.unwrap_or(Payload::None),
            topic: self.tp,
            position,
            id: self.id,
            correlation_id: self.cid,
            timestamp: self.ts,
            priority: self.pri.map(|p| p.min(u64::from(u8::MAX)) as u8),
            signature: None,
            frame,
        }
    }
}

/// An epoch as the server writes it: 8 hex digits.
pub(crate) fn epoch_of(text: &str) -> Option<u32> {
    if text.len() != 8 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    u32::from_str_radix(text, 16).ok()
}

/// A JSON message's fields.
pub(crate) fn json_envelope(text: &str) -> Result<Envelope<'_>, Dropped> {
    let text = text
        .strip_prefix("WSE")
        .or_else(|| text.strip_prefix('S'))
        .or_else(|| text.strip_prefix('U'))
        .filter(|rest| rest.starts_with('{'))
        .unwrap_or(text);
    let mut de = serde_json::Deserializer::from_str(text);
    let envelope = de.deserialize_map(EnvelopeVisitor).map_err(Dropped::Json)?;
    de.end().map_err(Dropped::Json)?;
    Ok(envelope)
}

/// The messages of a JSON batch payload (`{"messages": [...]}`), as text.
pub(crate) fn json_batch(payload: &str) -> Option<Vec<&str>> {
    #[derive(serde::Deserialize)]
    struct Batch<'a> {
        #[serde(borrow)]
        messages: Vec<&'a RawValue>,
    }
    serde_json::from_str::<Batch<'_>>(payload)
        .ok()
        .map(|batch| batch.messages.into_iter().map(RawValue::get).collect())
}

enum Field {
    T,
    P,
    C,
    Tp,
    E,
    O,
    Id,
    Cid,
    Ts,
    Pri,
    Sb,
    Sig,
    Kid,
    N,
    Other,
}

impl Field {
    fn of(name: &str) -> Self {
        match name {
            "t" | "type" => Self::T,
            "p" | "payload" => Self::P,
            "c" => Self::C,
            "tp" => Self::Tp,
            "e" => Self::E,
            "o" => Self::O,
            "id" => Self::Id,
            "cid" => Self::Cid,
            "ts" => Self::Ts,
            "pri" => Self::Pri,
            "sb" => Self::Sb,
            "sig" => Self::Sig,
            "kid" => Self::Kid,
            "n" => Self::N,
            _ => Self::Other,
        }
    }
}

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct FieldVisitor;
        impl Visitor<'_> for FieldVisitor {
            type Value = Field;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a field name")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Field, E> {
                Ok(Field::of(v))
            }
        }
        d.deserialize_identifier(FieldVisitor)
    }
}

/// A string field; a value of another type reads as absent.
struct Lenient<'a>(Option<Cow<'a, str>>);

impl<'de> Deserialize<'de> for Lenient<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Lenient<'de>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("any value")
            }
            fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<Self::Value, E> {
                Ok(Lenient(Some(Cow::Borrowed(v))))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(Lenient(Some(Cow::Owned(v.to_owned()))))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(Lenient(Some(Cow::Owned(v))))
            }
            fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
                Ok(Lenient(None))
            }
            fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
                Ok(Lenient(None))
            }
            fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self::Value, E> {
                Ok(Lenient(None))
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
                Ok(Lenient(None))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(Lenient(None))
            }
            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(Lenient(None))
            }
            fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
                Lenient::deserialize(d)
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(Lenient(None))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(Lenient(None))
            }
        }
        d.deserialize_any(V)
    }
}

/// An unsigned integer field; a value of another type reads as absent.
struct LenientU64(Option<u64>);

impl<'de> Deserialize<'de> for LenientU64 {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = LenientU64;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("any value")
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(LenientU64(Some(v)))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(LenientU64(u64::try_from(v).ok()))
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
                Ok(LenientU64(None))
            }
            fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
                Ok(LenientU64(None))
            }
            fn visit_str<E: de::Error>(self, _: &str) -> Result<Self::Value, E> {
                Ok(LenientU64(None))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(LenientU64(None))
            }
            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(LenientU64(None))
            }
            fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
                LenientU64::deserialize(d)
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(LenientU64(None))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(LenientU64(None))
            }
        }
        d.deserialize_any(V)
    }
}

struct EnvelopeVisitor;

impl<'de> Visitor<'de> for EnvelopeVisitor {
    type Value = Envelope<'de>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a message object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Envelope<'de>, A::Error> {
        let mut env = Envelope::default();
        while let Some(field) = map.next_key::<Field>()? {
            match field {
                Field::T => env.t = map.next_value::<Lenient<'de>>()?.0,
                Field::P => {
                    let raw = map.next_value::<&'de RawValue>()?.get();
                    env.p = (raw != "null").then_some(Payload::Json(raw));
                }
                Field::C => env.c = map.next_value::<Lenient<'de>>()?.0,
                Field::Tp => env.tp = map.next_value::<Lenient<'de>>()?.0,
                Field::E => env.e = map.next_value::<Lenient<'de>>()?.0,
                Field::O => env.o = map.next_value::<LenientU64>()?.0,
                Field::Id => env.id = map.next_value::<Lenient<'de>>()?.0,
                Field::Cid => env.cid = map.next_value::<Lenient<'de>>()?.0,
                Field::Ts => env.ts = map.next_value::<Lenient<'de>>()?.0,
                Field::Pri => env.pri = map.next_value::<LenientU64>()?.0,
                Field::Sb => env.sb = map.next_value::<Lenient<'de>>()?.0,
                Field::Sig => env.sig = map.next_value::<Lenient<'de>>()?.0,
                Field::Kid => env.kid = map.next_value::<Lenient<'de>>()?.0,
                Field::N => env.n = map.next_value::<Lenient<'de>>()?.0,
                Field::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(env)
    }
}

// ---------------------------------------------------------------------------
// msgpack, read in place
// ---------------------------------------------------------------------------

/// A cursor over a msgpack value.
struct Cursor<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn byte(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.at)?;
        self.at += 1;
        Some(b)
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(n)?;
        let bytes = self.buf.get(self.at..end)?;
        self.at = end;
        Some(bytes)
    }

    fn be(&mut self, n: usize) -> Option<u64> {
        Some(
            self.take(n)?
                .iter()
                .fold(0u64, |acc, &b| (acc << 8) | u64::from(b)),
        )
    }

    /// A map's entry count.
    fn map_len(&mut self) -> Option<usize> {
        match self.byte()? {
            b @ 0x80..=0x8f => Some(usize::from(b & 0x0f)),
            0xde => self.be(2).map(|n| n as usize),
            0xdf => self.be(4).map(|n| n as usize),
            _ => None,
        }
    }

    /// An array's element count.
    fn array_len(&mut self) -> Option<usize> {
        match self.byte()? {
            b @ 0x90..=0x9f => Some(usize::from(b & 0x0f)),
            0xdc => self.be(2).map(|n| n as usize),
            0xdd => self.be(4).map(|n| n as usize),
            _ => None,
        }
    }

    /// A string at the cursor, or `None` (with the value skipped) when the
    /// value is something else.
    fn str_or_skip(&mut self) -> Option<Option<&'a str>> {
        let len = match self.buf.get(self.at)? {
            b @ 0xa0..=0xbf => {
                self.at += 1;
                usize::from(b & 0x1f)
            }
            0xd9 => {
                self.at += 1;
                self.be(1)? as usize
            }
            0xda => {
                self.at += 1;
                self.be(2)? as usize
            }
            0xdb => {
                self.at += 1;
                self.be(4)? as usize
            }
            _ => {
                self.skip(0)?;
                return Some(None);
            }
        };
        Some(std::str::from_utf8(self.take(len)?).ok())
    }

    /// A non-negative integer at the cursor, or `None` (with the value
    /// skipped) when the value is something else.
    fn uint_or_skip(&mut self) -> Option<Option<u64>> {
        let v = match *self.buf.get(self.at)? {
            b @ 0x00..=0x7f => {
                self.at += 1;
                Some(u64::from(b))
            }
            0xcc => {
                self.at += 1;
                Some(self.be(1)?)
            }
            0xcd => {
                self.at += 1;
                Some(self.be(2)?)
            }
            0xce => {
                self.at += 1;
                Some(self.be(4)?)
            }
            0xcf => {
                self.at += 1;
                Some(self.be(8)?)
            }
            0xd0..=0xd3 => {
                let width = 1usize << (self.buf[self.at] - 0xd0);
                self.at += 1;
                let raw = self.be(width)?;
                let shift = 64 - 8 * width as u32;
                let signed = ((raw << shift) as i64) >> shift;
                u64::try_from(signed).ok()
            }
            _ => {
                self.skip(0)?;
                None
            }
        };
        Some(v)
    }

    /// Steps over one value, of any type.
    fn skip(&mut self, depth: u32) -> Option<()> {
        if depth > MAX_DEPTH {
            return None;
        }
        let (bytes, values) = match self.byte()? {
            0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => (0, 0),
            b @ 0x80..=0x8f => (0, 2 * usize::from(b & 0x0f)),
            b @ 0x90..=0x9f => (0, usize::from(b & 0x0f)),
            b @ 0xa0..=0xbf => (usize::from(b & 0x1f), 0),
            0xc4 | 0xd9 => (self.be(1)? as usize, 0),
            0xc5 | 0xda => (self.be(2)? as usize, 0),
            0xc6 | 0xdb => (self.be(4)? as usize, 0),
            0xc7 => (self.be(1)? as usize + 1, 0),
            0xc8 => (self.be(2)? as usize + 1, 0),
            0xc9 => (self.be(4)? as usize + 1, 0),
            0xca => (4, 0),
            0xcb => (8, 0),
            0xcc | 0xd0 => (1, 0),
            0xcd | 0xd1 => (2, 0),
            0xce | 0xd2 => (4, 0),
            0xcf | 0xd3 => (8, 0),
            0xd4 => (2, 0),
            0xd5 => (3, 0),
            0xd6 => (5, 0),
            0xd7 => (9, 0),
            0xd8 => (17, 0),
            0xdc => (0, self.be(2)? as usize),
            0xdd => (0, self.be(4)? as usize),
            0xde => (0, 2 * self.be(2)? as usize),
            0xdf => (0, 2 * self.be(4)? as usize),
            0xc1 => return None,
        };
        self.take(bytes)?;
        for _ in 0..values {
            self.skip(depth + 1)?;
        }
        Some(())
    }

    /// The bytes of the value at the cursor.
    fn value(&mut self) -> Option<&'a [u8]> {
        let start = self.at;
        self.skip(1)?;
        Some(&self.buf[start..self.at])
    }
}

/// A msgpack message's fields.
pub(crate) fn msgpack_envelope(buf: &[u8]) -> Result<Envelope<'_>, Dropped> {
    msgpack_fields(buf).ok_or(Dropped::Corrupt("msgpack message"))
}

fn msgpack_fields(buf: &[u8]) -> Option<Envelope<'_>> {
    let mut cur = Cursor { buf, at: 0 };
    let mut env = Envelope::default();
    for _ in 0..cur.map_len()? {
        let Some(key) = cur.str_or_skip()? else {
            cur.skip(1)?;
            continue;
        };
        match Field::of(key) {
            Field::T => env.t = string(&mut cur)?,
            Field::P => {
                let value = cur.value()?;
                env.p = (value != [0xc0]).then_some(Payload::Msgpack(value));
            }
            Field::C => env.c = string(&mut cur)?,
            Field::Tp => env.tp = string(&mut cur)?,
            Field::E => env.e = string(&mut cur)?,
            Field::O => env.o = cur.uint_or_skip()?,
            Field::Id => env.id = string(&mut cur)?,
            Field::Cid => env.cid = string(&mut cur)?,
            Field::Ts => env.ts = string(&mut cur)?,
            Field::Pri => env.pri = cur.uint_or_skip()?,
            Field::Sb => env.sb = string(&mut cur)?,
            Field::Sig => env.sig = string(&mut cur)?,
            Field::Kid => env.kid = string(&mut cur)?,
            Field::N => env.n = string(&mut cur)?,
            Field::Other => cur.skip(1)?,
        }
    }
    Some(env)
}

fn string<'a>(cur: &mut Cursor<'a>) -> Option<Option<Cow<'a, str>>> {
    Some(cur.str_or_skip()?.map(Cow::Borrowed))
}

/// The messages of a msgpack batch payload (`{"messages": [...]}`).
pub(crate) fn msgpack_batch(payload: &[u8]) -> Option<Vec<&[u8]>> {
    let mut cur = Cursor {
        buf: payload,
        at: 0,
    };
    for _ in 0..cur.map_len()? {
        match cur.str_or_skip()? {
            Some("messages") => {
                let n = cur.array_len()?;
                let mut messages = Vec::with_capacity(n.min(1024));
                for _ in 0..n {
                    messages.push(cur.value()?);
                }
                return Some(messages);
            }
            Some(_) => cur.skip(1)?,
            None => {
                cur.skip(1)?;
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(text: &str) -> Message<'_> {
        json_envelope(text).unwrap().into_message(None)
    }

    #[test]
    fn a_json_message_is_read_in_place() {
        let m = json(
            r#"{"tp":"prices","e":"0000abcd","o":42,"c":"U","t":"tick","p":{"px":1.5},"v":1}"#,
        );
        assert_eq!(m.kind(), "tick");
        assert_eq!(m.category(), Some("U"));
        assert_eq!(m.topic(), Some("prices"));
        assert_eq!(
            m.position(),
            Some(Position {
                epoch: 0xabcd,
                offset: 42
            })
        );
        assert_eq!(m.payload_json(), Some(r#"{"px":1.5}"#));
        assert!(matches!(m.kind, Cow::Borrowed(_)));
    }

    #[test]
    fn fields_of_another_type_read_as_absent() {
        let m = json(r#"{"t":"x","id":7,"o":"nope","tp":["a"],"e":{"k":1},"p":null}"#);
        assert_eq!(m.kind(), "x");
        assert_eq!(m.id(), None);
        assert_eq!(m.position(), None);
        assert!(matches!(m.payload, Payload::None));
        // An escaped string is read too, as a copy.
        let m = json(r#"{"t":"a\"b"}"#);
        assert_eq!(m.kind(), "a\"b");
    }

    #[test]
    fn a_stamp_needs_all_three_fields_and_an_epoch_of_eight_hex_digits() {
        assert_eq!(json(r#"{"t":"x","e":"0000abcd","o":1}"#).position(), None);
        assert_eq!(
            json(r#"{"t":"x","tp":"a","e":"abcd","o":1}"#).position(),
            None
        );
        assert_eq!(
            json(r#"{"t":"x","tp":"a","e":"+000abcd","o":1}"#).position(),
            None
        );
    }

    #[test]
    fn legacy_prefixes_and_keys_are_read() {
        let m = json(r#"WSE{"type":"server_ready","payload":{"a":1}}"#);
        assert_eq!(m.kind(), "server_ready");
        assert_eq!(m.payload_json(), Some(r#"{"a":1}"#));
        assert_eq!(json(r#"U{"t":"u"}"#).kind(), "u");
        assert!(json_envelope("[1,2]").is_err());
        assert!(json_envelope(r#"{"t":"x"} trailing"#).is_err());
    }

    #[test]
    fn a_batch_is_read_message_by_message() {
        let env = json_envelope(
            r#"{"t":"batch","p":{"messages":[{"t":"a","p":{"n":1}},{"t":"b"}],"count":2}}"#,
        )
        .unwrap();
        let Some(Payload::Json(p)) = env.p else {
            panic!("a JSON payload");
        };
        let kinds: Vec<String> = json_batch(p)
            .unwrap()
            .iter()
            .map(|raw| json(raw).kind().to_owned())
            .collect();
        assert_eq!(kinds, ["a", "b"]);
    }

    fn msgpack(value: &serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(value).unwrap()
    }

    #[test]
    fn a_msgpack_message_is_read_in_place() {
        let bytes = msgpack(&serde_json::json!({
            "tp": "prices", "e": "0000abcd", "o": 300, "c": "U", "t": "tick",
            "p": {"px": 1.5, "tags": ["a", {"b": null}]}, "seq": -5, "v": 1,
        }));
        let m = msgpack_envelope(&bytes).unwrap().into_message(None);
        assert_eq!(m.kind(), "tick");
        assert_eq!(m.topic(), Some("prices"));
        assert_eq!(
            m.position(),
            Some(Position {
                epoch: 0xabcd,
                offset: 300
            })
        );
        #[derive(serde::Deserialize, PartialEq, Debug)]
        struct Tick<'a> {
            px: f64,
            #[serde(borrow)]
            tags: (&'a str, serde_json::Value),
        }
        let tick: Tick<'_> = m.payload().unwrap();
        assert_eq!(tick.px, 1.5);
        // Borrowed from the frame.
        assert_eq!(tick.tags.0, "a");
        assert_eq!(
            m.payload_value().unwrap(),
            serde_json::json!({"px": 1.5, "tags": ["a", {"b": null}]})
        );
    }

    #[test]
    fn a_msgpack_batch_is_read_message_by_message() {
        let bytes = msgpack(&serde_json::json!({
            "t": "batch", "p": {"count": 2, "messages": [{"t": "a"}, {"t": "b", "p": {"n": 2}}]},
        }));
        let env = msgpack_envelope(&bytes).unwrap();
        let Some(Payload::Msgpack(p)) = env.p else {
            panic!("a msgpack payload");
        };
        let inner = msgpack_batch(p).unwrap();
        let second = msgpack_envelope(inner[1]).unwrap().into_message(None);
        assert_eq!(second.kind(), "b");
        assert_eq!(second.payload_value().unwrap(), serde_json::json!({"n": 2}));
    }

    #[test]
    fn broken_msgpack_is_refused_never_read_past_its_end() {
        let bytes = msgpack(&serde_json::json!({"t": "tick", "p": {"a": [1, 2, 3]}}));
        for cut in 0..bytes.len() {
            assert!(msgpack_envelope(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        // Nesting past the limit.
        let mut deep = vec![0x81, 0xa1, b'p'];
        deep.extend(std::iter::repeat_n(0x91, 100));
        deep.push(0x01);
        assert!(msgpack_envelope(&deep).is_err());
        // A huge declared count with nothing behind it.
        assert!(msgpack_envelope(&[0x81, 0xa1, b'p', 0xdd, 0xff, 0xff, 0xff, 0xff]).is_err());
    }

    fn zlib(data: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut z = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        z.write_all(data).unwrap();
        z.finish().unwrap()
    }

    #[test]
    fn compressed_frames_are_inflated_within_the_limit() {
        let message = format!(r#"{{"t":"big","p":{{"s":"{}"}}}}"#, "x".repeat(50_000));
        let mut frame = b"C:".to_vec();
        frame.extend(zlib(message.as_bytes()));
        let mut decoder = Decoder::new(1 << 20);
        let Ok(Body::Json(text)) = decoder.binary(&frame) else {
            panic!("JSON after inflating");
        };
        assert_eq!(text, message);
        // The same decoder again, then one with a lower limit.
        assert!(matches!(decoder.binary(&frame), Ok(Body::Json(_))));
        let mut small = Decoder::new(10_000);
        assert!(matches!(small.binary(&frame), Err(Dropped::TooBig)));
        // Cut short.
        assert!(decoder.binary(&frame[..frame.len() / 2]).is_err());
    }

    #[test]
    fn binary_frames_are_told_apart() {
        let mut decoder = Decoder::new(1 << 20);
        assert!(matches!(decoder.binary(b"{\"t\":1}"), Ok(Body::Json(_))));
        assert!(matches!(decoder.binary(b"M:\x80"), Ok(Body::Msgpack(_))));
        assert!(matches!(decoder.binary(b"E:\x01"), Ok(Body::Sealed)));
        assert!(matches!(
            decoder.binary(b"\x81\xa1t\xa1x"),
            Ok(Body::Msgpack(_))
        ));
        assert!(matches!(decoder.binary(b"\x00\x01raw"), Ok(Body::Binary)));
        assert!(matches!(decoder.binary(b"plain text"), Ok(Body::Binary)));
    }
}
