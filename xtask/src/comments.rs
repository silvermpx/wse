//! Comments are prose for the next reader, not the author's bookkeeping.
//!
//! A comment says why the code is the way it is, in plain English. Audit
//! finding ids, task and issue numbers, phase or wave numbers and the dates of
//! our own changes age at once and mean nothing to the next reader; the
//! commit log already holds them. Python docstrings count as comments.
//!
//! A line that has to quote such a shape (a standard's name the term list
//! misses, say) is marked `comment-kitchen:allow`; a comment that must quote
//! non-English text is marked `comment-language:allow`.

use std::sync::OnceLock;

use regex::Regex;

use crate::lexer::{self, Lang};

pub struct Finding {
    pub path: String,
    pub line: usize,
    pub rule: &'static str,
    pub text: String,
}

/// Hyphenated names that look like a task index but are standards, ciphers,
/// curves or encodings.
const TERMS: &[&str] = &[
    "AES", "ANSI", "ARM", "AVX", "BF", "C", "CRC", "CVE", "E", "ECDH", "ECDSA", "ECMA", "ED", "ES",
    "F", "FP", "G", "GHSA", "GCM", "H", "HKDF", "HMAC", "HS", "HTTP", "IEEE", "INT", "IPV", "ISO",
    "L", "M", "P", "PBKDF", "PEP", "PS", "RFC", "RS", "RSA", "RUSTSEC", "S", "SHA", "SSE", "TCP",
    "TLS", "UDP", "UTF", "UUID", "X",
];

/// Words that name a standard, so a `§` next to them cites a section of it.
const STANDARD: &str =
    r"(?i)\b(?:rfc|pep|iso|fips|nist|ieee|ecma|sp 800|noise|tls|quic|webcrypto|w3c)\b";

fn rules() -> &'static [(&'static str, Regex)] {
    static RULES: OnceLock<Vec<(&'static str, Regex)>> = OnceLock::new();
    RULES.get_or_init(|| {
        [
            // Audit finding ids (A92, A147) and review tags (R2 #3). comment-kitchen:allow
            ("audit-id", r"\bA\d{1,3}\b|\bR\d+ ?#\d+\b|\bR\d+ audit\b"),
            // Issue and pull request numbers: #75, (#7). comment-kitchen:allow
            ("issue-number", r"(?:^|[\s(])#\d+\b"),
            (
                "task-index",
                r"\b[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*-\d+(?:\.\d+)?\b",
            ),
            // Project plan numbers. Numbered steps of an algorithm or a test
            // ("Phase 1: connect") are prose and pass.
            (
                "plan-number",
                r"(?i)\b(?:wave|milestone|sprint|task)\s*#?\d+[a-z]?\b",
            ),
            (
                "date",
                r"\b(?:19|20)\d{2}-\d{2}-\d{2}\b|\b\d{2}\.\d{2}\.(?:19|20)\d{2}\b",
            ),
            ("document-number", r"\bdocs?[-/ ]\d{1,3}\b|§\s?\d"),
        ]
        .into_iter()
        .map(|(name, re)| (name, Regex::new(re).expect("a fixed pattern")))
        .collect()
    })
}

fn standard() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(STANDARD).expect("a fixed pattern"))
}

/// The bookkeeping shape a comment line carries, if any.
pub fn kitchen_shape(comment: &str) -> Option<&'static str> {
    for (name, re) in rules() {
        for m in re.find_iter(comment) {
            let hit = m.as_str();
            if *name == "task-index" {
                let head: String = hit
                    .chars()
                    .take_while(|c| c.is_ascii_alphabetic())
                    .collect();
                if TERMS.contains(&head.as_str()) {
                    continue;
                }
            }
            if *name == "document-number" && hit.starts_with('§') && standard().is_match(comment) {
                continue;
            }
            return Some(name);
        }
    }
    None
}

/// Whether a comment is written in a language other than English: any
/// Cyrillic letter (the code base has no Russian data to quote).
pub fn non_english(comment: &str) -> bool {
    comment
        .chars()
        .any(|c| ('\u{0400}'..='\u{04FF}').contains(&c))
}

pub fn check(path: &str, src: &str, lang: Lang) -> Vec<Finding> {
    let mut out = Vec::new();
    for line in lexer::split(src, lang) {
        let text = line.comment.trim();
        if text.is_empty() {
            continue;
        }
        if !text.contains("comment-kitchen:allow")
            && let Some(rule) = kitchen_shape(text)
        {
            out.push(Finding {
                path: path.to_string(),
                line: line.number,
                rule,
                text: text.to_string(),
            });
        }
        if !text.contains("comment-language:allow") && non_english(text) {
            out.push(Finding {
                path: path.to_string(),
                line: line.number,
                rule: "not-english",
                text: text.to_string(),
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bookkeeping_is_named_and_prose_passes() {
        for (text, want) in [
            ("the writer seals just before it writes", None),
            (
                "UTF-8, SHA-256, AES-GCM-256 and P-256 are names, not indexes",
                None,
            ),
            ("see RFC 8446 §5.5 for the AES-GCM limits", None),
            ("A92: the topic went dark after a gap", Some("audit-id")),
            ("anchor = last delivered (A144)", Some("audit-id")),
            ("R2 #3 asked for this", Some("audit-id")),
            ("fixes #75", Some("issue-number")),
            ("the wave 2 server work", Some("plan-number")),
            ("task 17 asked for it", Some("plan-number")),
            ("Phase 3: decode the frame", None),
            ("FIX-GATE-1 pinned this", Some("task-index")),
            ("pinned on 2026-10-16", Some("date")),
            ("see doc 12 §7", Some("document-number")),
            ("a p95 of 3 ms and 128 KiB frames are fine", None),
            ("the 0xA1 byte is the frame tag", None),
        ] {
            assert_eq!(kitchen_shape(text), want, "{text}");
        }
    }

    #[test]
    fn non_english_comments_are_caught() {
        assert!(non_english("ключ шифрования"));
        assert!(!non_english("the encryption key"));
    }

    #[test]
    fn markers_and_strings_are_respected() {
        let src =
            "let s = \"A92 lives in a string\"; // fine\n// A92 comment-kitchen:allow\n// A93\n";
        let found = check("x.rs", src, Lang::Rust);
        assert_eq!(found.len(), 1);
        assert_eq!((found[0].line, found[0].rule), (3, "audit-id"));
    }
}
