//! The cases, as `tests/conformance/cases.json` holds them: built in.

use serde::Deserialize;
use serde_json::Value;

/// The catalog.
#[derive(Deserialize)]
pub struct Catalog {
    pub cases: Vec<Case>,
}

/// One case: its server, its steps, its rules.
#[derive(Clone, Deserialize)]
pub struct Case {
    pub case: String,
    #[serde(default)]
    pub about: String,
    /// The features a client needs for it; one without them is not judged.
    #[serde(default)]
    pub needs: Vec<String>,
    #[serde(default)]
    pub server: ServerSettings,
    /// As the driver gets them: each step is the client's (`client`) or the
    /// server's (`server`), in its place.
    pub steps: Vec<Value>,
    pub rules: Vec<Rule>,
}

/// How the case's server is set up.
#[derive(Clone, Default, Deserialize)]
pub struct ServerSettings {
    /// Messages of history kept per topic; none without.
    pub recovery: Option<usize>,
    /// An E2E identity key, for the client to pin.
    #[serde(default)]
    pub e2e: bool,
    /// A signing key, for signed messages both ways.
    #[serde(default)]
    pub signing: bool,
}

/// A rule the judge holds the case to.
#[derive(Clone, Deserialize)]
pub struct Rule {
    pub rule: String,
    pub topic: Option<String>,
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub count: Option<usize>,
    pub payload: Option<Value>,
    /// `correlated`: the correlation id and priority a request named.
    pub correlation_id: Option<String>,
    pub priority: Option<u64>,
}

/// The built-in catalog.
pub fn catalog() -> Catalog {
    serde_json::from_str(include_str!("../../../tests/conformance/cases.json"))
        .expect("tests/conformance/cases.json is a catalog")
}
