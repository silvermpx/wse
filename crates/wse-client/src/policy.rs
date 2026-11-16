//! What follows a lost connection: another attempt after a wait, or the end
//! of the client. The same on every platform.

use tokio::sync::oneshot;

use crate::config::Config;
use crate::error::Error;
use crate::handler::Disconnected;

/// Close codes that mean the credentials were refused.
const AUTH_CLOSE_CODES: [u16; 3] = [4401, 4403, 1008];
/// No end-to-end suite in common: trying again is refused the same way.
const CLOSE_E2E_UNSUPPORTED: u16 = 4800;
/// Refused tokens in a row a token provider may replace before the client
/// gives up.
const AUTH_ATTEMPTS: u32 = 3;

/// How a connection ended.
pub(crate) enum Lost {
    /// The application closed the client.
    Shutdown,
    /// The application asked for a new connection.
    Requested,
    /// The application paused the client.
    Paused,
    /// The connection went away.
    Closed { code: Option<u16>, reason: String },
    /// An attempt failed.
    Failed(Error),
    /// The client cannot go on.
    Fatal(Error),
}

/// What follows.
pub(crate) enum Next {
    /// The client stops: closed by the application (`None`), or the reason.
    Stop(Option<Error>),
    /// Another attempt, after `retry_in`.
    Retry(Disconnected),
}

/// The attempts and refused tokens since the last connection the server
/// took.
#[derive(Default)]
pub(crate) struct Policy {
    attempts: u32,
    auth_failures: u32,
}

/// The first connection's answer, for `connect`.
pub(crate) type First = Option<oneshot::Sender<Result<(), Error>>>;

impl Policy {
    /// The server took a connection: the counts start over.
    pub(crate) fn connected(&mut self) {
        self.attempts = 0;
        self.auth_failures = 0;
    }

    /// What follows `lost`. The first connection is the caller's to retry:
    /// `first` gets its error and the client stops.
    pub(crate) fn after(&mut self, lost: Lost, config: &Config, first: &mut First) -> Next {
        let (code, reason, error) = match lost {
            Lost::Shutdown => return Next::Stop(None),
            // Asked for: at once, whatever the reconnect settings say, and
            // the counts start over.
            Lost::Requested | Lost::Paused => {
                self.connected();
                return Next::Retry(Disconnected {
                    code: None,
                    reason: "reconnect asked".to_owned(),
                    retry_in: std::time::Duration::ZERO,
                    attempt: 0,
                });
            }
            Lost::Closed { code, reason } => {
                let error = close_error(code, &reason);
                (code, reason, error)
            }
            Lost::Failed(error) => (None, error.to_string(), error),
            Lost::Fatal(error) => {
                return match first.take() {
                    Some(first) => {
                        let _ = first.send(Err(error));
                        Next::Stop(None)
                    }
                    None => Next::Stop(Some(error)),
                };
            }
        };
        if let Some(first) = first.take() {
            let _ = first.send(Err(error));
            return Next::Stop(None);
        }
        match &error {
            Error::Auth(_) => {
                self.auth_failures += 1;
                if config.token_provider.is_none() || self.auth_failures >= AUTH_ATTEMPTS {
                    return Next::Stop(Some(error));
                }
            }
            Error::Encryption(_) | Error::Config(_) => return Next::Stop(Some(error)),
            _ => {}
        }
        if !config.reconnect.enabled {
            return Next::Stop(Some(error));
        }
        if let Some(max) = config.reconnect.max_attempts
            && self.attempts >= max
        {
            return Next::Stop(Some(Error::GaveUp(format!(
                "{} attempt(s); last: {error}",
                self.attempts
            ))));
        }
        let retry_in = config.reconnect.delay(self.attempts);
        self.attempts += 1;
        tracing::info!(
            "[WSE] connection lost ({reason}); attempt {} in {retry_in:?}",
            self.attempts
        );
        Next::Retry(Disconnected {
            code,
            reason,
            retry_in,
            attempt: self.attempts,
        })
    }
}

/// What a close means for the client.
fn close_error(code: Option<u16>, reason: &str) -> Error {
    let what = match code {
        Some(code) if reason.is_empty() => format!("closed with {code}"),
        Some(code) => format!("closed with {code}: {reason}"),
        None => reason.to_owned(),
    };
    match code {
        Some(code) if AUTH_CLOSE_CODES.contains(&code) => Error::Auth(what),
        Some(CLOSE_E2E_UNSUPPORTED) => Error::Encryption(what),
        _ => Error::Connect(what),
    }
}
