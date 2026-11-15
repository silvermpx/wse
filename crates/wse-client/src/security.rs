//! End-to-end sessions and signed messages, the client's side. The crypto
//! and the session are the server's own (`wse::protocol`).

#[cfg(feature = "aws-lc")]
pub(crate) use with_crypto::*;
#[cfg(not(feature = "aws-lc"))]
pub(crate) use without_crypto::*;

#[cfg(feature = "aws-lc")]
pub(crate) mod with_crypto {
    use base64::Engine;
    use wse::protocol::e2e::{ClientOffer, E2eError, RecvState, SendState};
    use wse::protocol::signing::{CLIENT_LABEL, PublicKey, SERVER_LABEL, Signer};

    use crate::config::Config;
    use crate::error::Error;

    fn b64() -> base64::engine::GeneralPurpose {
        base64::engine::general_purpose::STANDARD
    }

    /// The keys of a client, read from its configuration once.
    pub(crate) struct Keys {
        /// Open end-to-end sessions.
        pub(crate) e2e: bool,
        /// The server's E2E identity keys, uncompressed points.
        identities: Vec<Vec<u8>>,
        unauthenticated: bool,
        /// The server's signing keys its signed messages are checked
        /// against.
        server: Vec<PublicKey>,
        /// This client's signing key.
        signer: Option<Signer>,
        /// Sessions are bound to the signing key.
        bind_session: bool,
    }

    impl Keys {
        pub(crate) fn from_config(config: &Config) -> Result<Self, Error> {
            let point = |key: &str, what: &str| {
                b64()
                    .decode(key.trim())
                    .ok()
                    .filter(|p| p.len() == 65 && p[0] == 4)
                    .ok_or_else(|| {
                        Error::Config(format!(
                            "{what} is base64 of an uncompressed P-256 point: {key}"
                        ))
                    })
            };
            let bind_session = config
                .encryption
                .as_ref()
                .is_some_and(|encryption| encryption.bind_session);
            let (e2e, identities, unauthenticated) = match &config.encryption {
                None => (false, Vec::new(), false),
                Some(encryption) => {
                    if encryption.server_keys.is_empty() && !encryption.unauthenticated {
                        return Err(Error::Config(
                            "encryption needs the server's identity keys (server_keys), \
                             or unauthenticated"
                                .to_owned(),
                        ));
                    }
                    let identities = encryption
                        .server_keys
                        .iter()
                        .map(|k| point(k, "an E2E server key"))
                        .collect::<Result<_, _>>()?;
                    (true, identities, encryption.unauthenticated)
                }
            };
            let server = config
                .server_signing_keys
                .iter()
                .map(|k| {
                    PublicKey::from_base64(k).ok_or_else(|| {
                        Error::Config(format!(
                            "a server signing key is base64 of an uncompressed P-256 point: {k}"
                        ))
                    })
                })
                .collect::<Result<_, _>>()?;
            let signer = config
                .signing_key
                .as_deref()
                .map(|key| {
                    Signer::from_pkcs8(key).map_err(|_| {
                        Error::Config(
                            "signing_key is not an ECDSA P-256 private key in PKCS#8".to_owned(),
                        )
                    })
                })
                .transpose()?;
            Ok(Self {
                bind_session: bind_session && e2e && signer.is_some(),
                e2e,
                identities,
                unauthenticated,
                server,
                signer,
            })
        }

        /// The proof of this client's key over the session `th`
        /// (`client_auth`), when its sessions are bound to it.
        pub(crate) fn prove_session(&self, th: &[u8; 32]) -> Result<Option<String>, Error> {
            match &self.signer {
                Some(signer) if self.bind_session => signer
                    .prove_session(th)
                    .map(Some)
                    .map_err(|e| Error::Encryption(format!("the session proof: {e}"))),
                _ => Ok(None),
            }
        }

        /// The thumbprint the server must confirm a session is bound to,
        /// when sessions are.
        pub(crate) fn bound_thumbprint(&self) -> Option<String> {
            self.signer
                .as_ref()
                .filter(|_| self.bind_session)
                .map(Signer::thumbprint)
        }

        /// A server-signed message: verified against a pinned key (true),
        /// or unchecked when none is pinned (false).
        pub(crate) fn check_server(
            &self,
            kid: &str,
            body: &str,
            sig: &str,
        ) -> Result<bool, String> {
            if self.server.is_empty() {
                return Ok(false);
            }
            let key = self
                .server
                .iter()
                .find(|k| k.kid == kid)
                .ok_or_else(|| format!("no pinned server signing key {kid}"))?;
            let sig = b64()
                .decode(sig)
                .map_err(|_| "the signature is not base64".to_owned())?;
            if key.verifies(SERVER_LABEL, body, &sig) {
                Ok(true)
            } else {
                Err("the server's signature does not verify".to_owned())
            }
        }

        /// Whether this client signs for the server key `kid`: one it pins,
        /// when it pins any.
        pub(crate) fn signs_for(&self, kid: &str) -> bool {
            self.server.is_empty() || self.server.iter().any(|k| k.kid == kid)
        }

        /// A signed message `{"t": kind, "p": payload}` for the server key
        /// `audience`.
        pub(crate) fn sign(
            &self,
            kind: &str,
            payload: &serde_json::value::RawValue,
            audience: &str,
            n: &str,
            now_ms: u64,
        ) -> Result<String, Error> {
            #[derive(serde::Serialize)]
            struct Body<'a> {
                t: &'a str,
                p: &'a serde_json::value::RawValue,
                aud: &'a str,
                ts: u64,
                n: &'a str,
            }
            let signer = self
                .signer
                .as_ref()
                .ok_or_else(|| Error::Signing("no signing_key configured".to_owned()))?;
            let body = serde_json::to_string(&Body {
                t: kind,
                p: payload,
                aud: audience,
                ts: now_ms,
                n,
            })
            .map_err(|e| Error::Encode(e.to_string()))?;
            signer
                .sign_envelope(CLIENT_LABEL, &body)
                .map_err(|_| Error::Signing("the signing key failed".to_owned()))
        }

        /// A fresh offer for a connection, when it opens a session.
        pub(crate) fn offer(&self) -> Result<Option<Sealing>, Error> {
            if !self.e2e {
                return Ok(None);
            }
            let offer = ClientOffer::new().map_err(|e| Error::Encryption(e.to_string()))?;
            Ok(Some(Sealing {
                offer: Some(offer),
                send: None,
                recv: None,
            }))
        }
    }

    /// A new ECDSA P-256 private key, PKCS#8 DER: a client's signing key
    /// ([`Config::signing_key`](crate::Config::signing_key)), or a key for a
    /// server.
    pub fn generate_key() -> Vec<u8> {
        wse::protocol::crypto::generate_p256_pkcs8().expect("the system's random generator works")
    }

    /// The public key of a PKCS#8 private key, base64 of the uncompressed
    /// point: what a server binds to a connection (`set_client_key`), and
    /// what clients pin.
    pub fn public_key(pkcs8: &[u8]) -> Result<String, Error> {
        let key = wse::protocol::crypto::IdentityKey::from_pkcs8(pkcs8)
            .map_err(|_| Error::Config("not an ECDSA P-256 private key in PKCS#8".to_owned()))?;
        Ok(b64().encode(key.public_key()))
    }

    /// The public key of a PKCS#8 private key as a JWK, for the `cnf` claim
    /// of the token a server issues the client (RFC 7800): the server then
    /// takes the client's signed messages without being told the key.
    pub fn public_key_jwk(pkcs8: &[u8]) -> Result<serde_json::Value, Error> {
        let key = wse::protocol::crypto::IdentityKey::from_pkcs8(pkcs8)
            .map_err(|_| Error::Config("not an ECDSA P-256 private key in PKCS#8".to_owned()))?;
        let point = key.public_key();
        let url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        Ok(serde_json::json!({
            "kty": "EC",
            "crv": "P-256",
            "x": url.encode(&point[1..33]),
            "y": url.encode(&point[33..]),
        }))
    }

    /// One connection's end-to-end session: the offer until the server
    /// answers, then a key schedule per direction.
    pub(crate) struct Sealing {
        offer: Option<ClientOffer>,
        send: Option<SendState>,
        recv: Option<RecvState>,
    }

    impl Sealing {
        /// `client_hello.p.e2e`.
        pub(crate) fn hello(&self) -> Option<serde_json::Value> {
            self.offer.as_ref().map(ClientOffer::message)
        }

        /// The session is open.
        pub(crate) fn is_open(&self) -> bool {
            self.recv.is_some()
        }

        /// The hash of the session's handshake, once it is open.
        pub(crate) fn transcript_hash(&self) -> Option<[u8; 32]> {
            self.send.as_ref().map(SendState::transcript_hash)
        }

        /// Opens the session with the server's answer.
        pub(crate) fn accept(
            &mut self,
            keys: &Keys,
            hello: &serde_json::Value,
            connection_id: &str,
        ) -> Result<(), Error> {
            let offer = self
                .offer
                .take()
                .ok_or_else(|| Error::Encryption("a second server_hello".to_owned()))?;
            let (send, recv) = offer
                .accept(hello, connection_id, &keys.identities, keys.unauthenticated)
                .map_err(|e| Error::Encryption(e.to_string()))?;
            self.send = Some(send);
            self.recv = Some(recv);
            Ok(())
        }

        /// The plaintext of the next sealed frame; `None` for the server's
        /// close notice.
        pub(crate) fn open(&mut self, frame: &[u8]) -> Result<Option<Vec<u8>>, Error> {
            let recv = self
                .recv
                .as_mut()
                .ok_or_else(|| Error::Encryption("a sealed frame before the session".to_owned()))?;
            recv.open(frame)
                .map_err(|e| Error::Encryption(e.to_string()))
        }

        /// `text` sealed, once the session is open; as it is before.
        pub(crate) fn seal(&mut self, text: &str) -> Result<Option<Vec<u8>>, Error> {
            match self.send.as_mut() {
                Some(send) => send.seal(text.as_bytes()).map(Some).map_err(|e| match e {
                    E2eError::Exhausted => {
                        Error::Connect("the e2e frame counter is used up".to_owned())
                    }
                    e => Error::Encryption(e.to_string()),
                }),
                None => Ok(None),
            }
        }
    }
}

#[cfg(not(feature = "aws-lc"))]
mod without_crypto {
    use crate::config::Config;
    use crate::error::Error;

    /// Without a crypto library: no sessions, signed messages unchecked.
    pub(crate) struct Keys {
        pub(crate) e2e: bool,
    }

    impl Keys {
        pub(crate) fn from_config(_config: &Config) -> Result<Self, Error> {
            Ok(Self { e2e: false })
        }

        pub(crate) fn check_server(&self, _: &str, _: &str, _: &str) -> Result<bool, String> {
            Ok(false)
        }

        pub(crate) fn signs_for(&self, _: &str) -> bool {
            false
        }

        pub(crate) fn offer(&self) -> Result<Option<Sealing>, Error> {
            Ok(None)
        }

        pub(crate) fn prove_session(&self, _: &[u8; 32]) -> Result<Option<String>, Error> {
            Ok(None)
        }

        pub(crate) fn bound_thumbprint(&self) -> Option<String> {
            None
        }
    }

    pub(crate) enum Sealing {}

    impl Sealing {
        pub(crate) fn hello(&self) -> Option<serde_json::Value> {
            match *self {}
        }
        pub(crate) fn is_open(&self) -> bool {
            match *self {}
        }
        pub(crate) fn transcript_hash(&self) -> Option<[u8; 32]> {
            match *self {}
        }
        pub(crate) fn accept(
            &mut self,
            _: &Keys,
            _: &serde_json::Value,
            _: &str,
        ) -> Result<(), Error> {
            match *self {}
        }
        pub(crate) fn open(&mut self, _: &[u8]) -> Result<Option<Vec<u8>>, Error> {
            match *self {}
        }
        pub(crate) fn seal(&mut self, _: &str) -> Result<Option<Vec<u8>>, Error> {
            match *self {}
        }
    }
}
