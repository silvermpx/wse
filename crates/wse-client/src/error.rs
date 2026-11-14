use std::fmt;

/// Why a call failed or the client stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The configuration cannot work: a URL that is not `ws://` or
    /// `wss://`, a header that cannot be sent.
    Config(String),
    /// The server could not be reached or did not upgrade the connection.
    Connect(String),
    /// The server refused the credentials: at the upgrade (HTTP 401 or
    /// 403), or by closing with 4401, 4403 or 1008.
    Auth(String),
    /// The server did not confirm the connection in time.
    Timeout(String),
    /// The end-to-end session could not be opened or went wrong.
    Encryption(String),
    /// A signed message cannot be sent: no signing key, or no server key
    /// to sign for.
    Signing(String),
    /// The client gave up reconnecting.
    GaveUp(String),
    /// Not connected now: the client is reconnecting.
    NotConnected,
    /// The send queue is full.
    Full,
    /// The client was closed.
    Closed,
    /// A message could not be encoded.
    Encode(String),
    /// A payload did not decode into the type asked for.
    Decode(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(why) => write!(f, "invalid configuration: {why}"),
            Self::Connect(why) => write!(f, "cannot connect: {why}"),
            Self::Auth(why) => write!(f, "authentication refused: {why}"),
            Self::Timeout(why) => write!(f, "timed out: {why}"),
            Self::Encryption(why) => write!(f, "end-to-end encryption failed: {why}"),
            Self::Signing(why) => write!(f, "cannot sign: {why}"),
            Self::GaveUp(why) => write!(f, "gave up reconnecting: {why}"),
            Self::NotConnected => f.write_str("not connected"),
            Self::Full => f.write_str("the send queue is full"),
            Self::Closed => f.write_str("the client is closed"),
            Self::Encode(why) => write!(f, "cannot encode the message: {why}"),
            Self::Decode(why) => write!(f, "cannot decode the payload: {why}"),
        }
    }
}

impl std::error::Error for Error {}
