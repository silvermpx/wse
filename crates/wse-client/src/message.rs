//! What the server sends, read in place: a [`Message`] borrows its frame,
//! an [`OwnedMessage`] keeps it.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use serde::Deserialize;

use bytestring::ByteString;

use crate::error::Error;

/// Where a topic message stands in its topic: the epoch of the topic's
/// history on the server that published it, and the offset within it.
///
/// A client that comes back asks for its topics from their last positions
/// and gets what it missed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Position {
    pub epoch: u32,
    pub offset: u64,
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:08x}:{}", self.epoch, self.offset)
    }
}

/// What proves who sent a signed message: keep all three.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Signature {
    /// The exact text that was signed: a JSON object.
    pub body: String,
    /// The ECDSA P-256 signature, `r || s` in base64.
    pub sig: String,
    /// The signing key's id: 8 bytes of SHA-256 of its public key, in hex.
    pub kid: String,
    /// Checked against a pinned server signing key; false when none is
    /// pinned.
    pub verified: bool,
}

/// The payload as it came: JSON text or a msgpack value.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Payload<'a> {
    None,
    Json(&'a str),
    Msgpack(&'a [u8]),
}

/// A message from the server, read in place from its frame: nothing is
/// copied until the application asks for it.
///
/// [`payload`](Self::payload) decodes the payload into the application's own
/// type, borrowing from the frame where the type does.
#[derive(Clone, Debug)]
pub struct Message<'a> {
    pub(crate) kind: Cow<'a, str>,
    pub(crate) category: Option<Cow<'a, str>>,
    pub(crate) payload: Payload<'a>,
    pub(crate) topic: Option<Cow<'a, str>>,
    pub(crate) position: Option<Position>,
    pub(crate) id: Option<Cow<'a, str>>,
    pub(crate) correlation_id: Option<Cow<'a, str>>,
    pub(crate) timestamp: Option<Cow<'a, str>>,
    pub(crate) priority: Option<u8>,
    pub(crate) signature: Option<Arc<Signature>>,
    /// The frame the message was read from, when it is one the message can
    /// keep without a copy.
    pub(crate) frame: Option<&'a Bytes>,
    /// The user generation it was handed out in: a commit counts in it only.
    pub(crate) generation: u32,
}

impl<'a> Message<'a> {
    /// The message type (`t`).
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// The category (`c`): `"U"` for updates, `"S"` for snapshots, `"WSE"`
    /// for the server's own messages.
    pub fn category(&self) -> Option<&str> {
        self.category.as_deref()
    }

    /// The topic a topic message was published to.
    pub fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    /// The message's position in its topic, when the server keeps the
    /// topic's history.
    pub fn position(&self) -> Option<Position> {
        self.position
    }

    /// The message id (`id`).
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// The correlation id (`cid`) pairing a reply with its request.
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    /// When the server sent it (`ts`, ISO 8601).
    pub fn timestamp(&self) -> Option<&str> {
        self.timestamp.as_deref()
    }

    /// The priority (`pri`): 1 background, 3 low, 5 normal, 8 high, 10
    /// critical.
    pub fn priority(&self) -> Option<u8> {
        self.priority
    }

    /// For a message the server signed: the proof.
    pub fn signature(&self) -> Option<&Signature> {
        self.signature.as_deref()
    }

    /// The payload (`p`) decoded into `T`, from JSON or msgpack as the
    /// connection receives it. A message without a payload decodes as an
    /// empty map.
    pub fn payload<T: Deserialize<'a>>(&self) -> Result<T, Error> {
        decode_payload(self.payload)
    }

    /// The payload's JSON text, on a JSON connection.
    pub fn payload_json(&self) -> Option<&'a str> {
        match self.payload {
            Payload::Json(text) => Some(text),
            _ => None,
        }
    }

    /// The payload as a JSON value, whatever the wire format.
    pub fn payload_value(&self) -> Result<serde_json::Value, Error> {
        self.payload()
    }

    /// A copy the application can keep. The frame itself is shared, not
    /// copied, where it can be.
    pub fn to_owned(&self) -> OwnedMessage {
        let frame = self.frame;
        let text = |s: &Option<Cow<'a, str>>| s.as_deref().map(|s| keep_str(frame, s));
        OwnedMessage {
            kind: keep_str(frame, &self.kind),
            category: text(&self.category),
            topic: text(&self.topic),
            id: text(&self.id),
            correlation_id: text(&self.correlation_id),
            timestamp: text(&self.timestamp),
            position: self.position,
            priority: self.priority,
            signature: self.signature.clone(),
            payload: match self.payload {
                Payload::None => OwnedPayload::None,
                Payload::Json(text) => OwnedPayload::Json(keep_str(frame, text)),
                Payload::Msgpack(bytes) => OwnedPayload::Msgpack(keep(frame, bytes)),
            },
            generation: self.generation,
        }
    }
}

fn decode_payload<'a, T: Deserialize<'a>>(payload: Payload<'a>) -> Result<T, Error> {
    match payload {
        Payload::Json(text) => serde_json::from_str(text).map_err(|e| Error::Decode(e.to_string())),
        Payload::Msgpack(bytes) => {
            rmp_serde::from_slice(bytes).map_err(|e| Error::Decode(e.to_string()))
        }
        Payload::None => serde_json::from_str("{}").map_err(|e| Error::Decode(e.to_string())),
    }
}

/// `part` as bytes the owned message keeps: a slice of the frame when it is
/// one, else a copy.
fn keep(frame: Option<&Bytes>, part: &[u8]) -> Bytes {
    match frame {
        Some(frame) if within(frame, part) => frame.slice_ref(part),
        _ => Bytes::copy_from_slice(part),
    }
}

fn keep_str(frame: Option<&Bytes>, part: &str) -> Text {
    let bytes = keep(frame, part.as_bytes());
    // SAFETY: `keep` returns the bytes of `part`, a slice of the frame or a
    // copy: those of a `str`, UTF-8 already.
    Text(unsafe { ByteString::from_bytes_unchecked(bytes) })
}

fn within(frame: &[u8], part: &[u8]) -> bool {
    let start = frame.as_ptr() as usize;
    let at = part.as_ptr() as usize;
    at >= start && at + part.len() <= start + frame.len()
}

/// UTF-8 text kept as shared bytes, checked once.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct Text(ByteString);

impl Text {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Text {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

#[derive(Clone, Debug)]
pub(crate) enum OwnedPayload {
    None,
    Json(Text),
    Msgpack(Bytes),
}

/// A message the application keeps: [`Message::to_owned`], or what the
/// [`Events`](crate::Events) stream yields.
#[derive(Clone, Debug)]
pub struct OwnedMessage {
    kind: Text,
    category: Option<Text>,
    topic: Option<Text>,
    id: Option<Text>,
    correlation_id: Option<Text>,
    timestamp: Option<Text>,
    position: Option<Position>,
    priority: Option<u8>,
    signature: Option<Arc<Signature>>,
    payload: OwnedPayload,
    /// The user generation it was handed out in.
    pub(crate) generation: u32,
}

impl OwnedMessage {
    /// A message of `kind` on `topic` at `position`, without a payload.
    #[cfg(test)]
    pub(crate) fn for_test(kind: &str, topic: Option<&str>, position: Option<Position>) -> Self {
        Self {
            kind: Text(ByteString::from(kind)),
            category: None,
            topic: topic.map(|t| Text(ByteString::from(t))),
            id: None,
            correlation_id: None,
            timestamp: None,
            position,
            priority: None,
            signature: None,
            payload: OwnedPayload::None,
            generation: 0,
        }
    }

    /// The message type (`t`).
    pub fn kind(&self) -> &str {
        self.kind.as_str()
    }

    /// The category (`c`).
    pub fn category(&self) -> Option<&str> {
        self.category.as_ref().map(Text::as_str)
    }

    /// The topic a topic message was published to.
    pub fn topic(&self) -> Option<&str> {
        self.topic.as_ref().map(Text::as_str)
    }

    /// The message's position in its topic.
    pub fn position(&self) -> Option<Position> {
        self.position
    }

    /// The message id (`id`).
    pub fn id(&self) -> Option<&str> {
        self.id.as_ref().map(Text::as_str)
    }

    /// The correlation id (`cid`).
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_ref().map(Text::as_str)
    }

    /// When the server sent it (`ts`).
    pub fn timestamp(&self) -> Option<&str> {
        self.timestamp.as_ref().map(Text::as_str)
    }

    /// The priority (`pri`).
    pub fn priority(&self) -> Option<u8> {
        self.priority
    }

    /// For a message the server signed: the proof.
    pub fn signature(&self) -> Option<&Signature> {
        self.signature.as_deref()
    }

    /// The payload (`p`) decoded into `T`.
    pub fn payload<'a, T: Deserialize<'a>>(&'a self) -> Result<T, Error> {
        decode_payload(self.payload_ref())
    }

    /// The payload's JSON text, on a JSON connection.
    pub fn payload_json(&self) -> Option<&str> {
        match &self.payload {
            OwnedPayload::Json(text) => Some(text.as_str()),
            _ => None,
        }
    }

    /// The message carries a payload (`p`), whatever it is.
    #[cfg_attr(not(feature = "native"), allow(dead_code))]
    pub(crate) fn has_payload(&self) -> bool {
        !matches!(self.payload, OwnedPayload::None)
    }

    fn payload_ref(&self) -> Payload<'_> {
        match &self.payload {
            OwnedPayload::None => Payload::None,
            OwnedPayload::Json(text) => Payload::Json(text.as_str()),
            OwnedPayload::Msgpack(bytes) => Payload::Msgpack(bytes),
        }
    }

    /// The payload as a JSON value, whatever the wire format.
    pub fn payload_value(&self) -> Result<serde_json::Value, Error> {
        self.payload()
    }

    /// The message as a [`Message`] again, borrowing from this one.
    pub fn as_message(&self) -> Message<'_> {
        fn text(t: &Option<Text>) -> Option<Cow<'_, str>> {
            t.as_ref().map(|t| Cow::Borrowed(t.as_str()))
        }
        Message {
            kind: Cow::Borrowed(self.kind.as_str()),
            category: text(&self.category),
            topic: text(&self.topic),
            id: text(&self.id),
            correlation_id: text(&self.correlation_id),
            timestamp: text(&self.timestamp),
            position: self.position,
            priority: self.priority,
            payload: self.payload_ref(),
            signature: self.signature.clone(),
            frame: None,
            generation: self.generation,
        }
    }
}
