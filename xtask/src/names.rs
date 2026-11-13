//! No version numbers in names.
//!
//! A function, type, module or file named `seal_v2`, `HandlerV3` or
//! `protocol_v2.rs` says only that something came before it; name it by what
//! it does instead. A wire protocol may still carry its version as data
//! (`protocol_version: 2`): that is a value, not a name.
//!
//! Library APIs that carry a version in their own name (`Uuid::now_v7`) are
//! not ours to rename.

use std::sync::OnceLock;

use regex::Regex;

use crate::lexer::{self, Lang};

pub struct Finding {
    pub path: String,
    pub line: usize,
    pub name: String,
}

/// Names from libraries we call.
const LIBRARY_NAMES: &[&str] = &["now_v7", "new_v4", "new_v7", "get_version_num"];

fn ident() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b[A-Za-z_][A-Za-z0-9_]*\b").expect("a fixed pattern"))
}

/// The words of a name: split at `_`, `-`, `.` and at camelCase humps.
fn words(name: &str) -> Vec<&str> {
    let mut out = Vec::new();
    for part in name.split(['_', '-', '.']).filter(|p| !p.is_empty()) {
        let bytes = part.as_bytes();
        let mut start = 0;
        for i in 1..bytes.len() {
            let hump = bytes[i - 1].is_ascii_lowercase() || bytes[i - 1].is_ascii_digit();
            if bytes[i].is_ascii_uppercase() && hump {
                out.push(&part[start..i]);
                start = i;
            }
        }
        out.push(&part[start..]);
    }
    out
}

fn is_version_word(word: &str) -> bool {
    let mut chars = word.chars();
    matches!(chars.next(), Some('v' | 'V')) && word.len() > 1 && chars.all(|c| c.is_ascii_digit())
}

/// Whether a name carries a version number (`seal_v2`, `HandlerV3`). A
/// version word alone (`v2`, the second value; `IpAddr::V4`) is not one, nor
/// is a UUID's version (`UUID_V4`).
pub fn versioned(name: &str) -> bool {
    if LIBRARY_NAMES.contains(&name) {
        return false;
    }
    let words = words(name);
    words.len() > 1
        && words.iter().enumerate().any(|(i, w)| {
            is_version_word(w) && !(i > 0 && words[i - 1].eq_ignore_ascii_case("uuid"))
        })
}

pub fn check_code(path: &str, src: &str, lang: Lang) -> Vec<Finding> {
    let mut out = Vec::new();
    for line in lexer::split(src, lang) {
        if line.comment.contains("names:allow") {
            continue;
        }
        for m in ident().find_iter(&line.code) {
            let name = m.as_str();
            if versioned(name) {
                out.push(Finding {
                    path: path.to_string(),
                    line: line.number,
                    name: name.to_string(),
                });
            }
        }
    }
    out
}

/// A file whose name carries a version number (`handler_v2.ts`).
pub fn versioned_file(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    let stem = name.rsplit_once('.').map_or(name, |(stem, _)| stem);
    words(stem).iter().any(|w| is_version_word(w))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_numbers_in_names_are_found() {
        for name in ["seal_v2", "HandlerV3", "decode_frame_v1", "E2eV2Session"] {
            assert!(versioned(name), "{name}");
        }
        for name in [
            "port2",
            "t0",
            "run_test_1",
            "sha256",
            "now_v7",
            "ipv4",
            "Ipv6Addr",
            "dev",
            "v",
            "v2",
            "V4",
            "UUID_V4",
        ] {
            assert!(!versioned(name), "{name}");
        }
    }

    #[test]
    fn strings_and_comments_are_not_read() {
        let src = "let x = \"seal_v2\"; // the v2 scheme\nfn seal_v2() {}\n";
        let found = check_code("x.rs", src, Lang::Rust);
        assert_eq!(found.len(), 1);
        assert_eq!((found[0].line, found[0].name.as_str()), (2, "seal_v2"));
    }

    #[test]
    fn file_names_with_a_version_are_found() {
        assert!(versioned_file("ts-client/handler_v2.ts"));
        assert!(versioned_file("rust/src/e2e-v3.rs"));
        assert!(!versioned_file("tests/keys/rsa2_private.pem"));
        assert!(!versioned_file("rust/src/wse/cluster/tls.rs"));
    }
}
