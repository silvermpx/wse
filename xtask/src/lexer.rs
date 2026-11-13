//! Tells comments from code and string data, line by line.
//!
//! Not a parser: just enough of Rust, TypeScript, Python and the hash-comment
//! formats (shell, YAML, TOML) that the checks read only what people wrote as
//! prose, and the architecture check reads only code. String contents are
//! blanked out of the code, so a path or a word inside a string never counts
//! as a reference.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lang {
    Rust,
    TypeScript,
    Python,
    /// `#` comments: shell, YAML, TOML.
    Hash,
}

impl Lang {
    pub fn of(path: &str) -> Option<Lang> {
        let name = path.rsplit('/').next().unwrap_or(path);
        let ext = name.rsplit_once('.').map(|(_, e)| e).unwrap_or("");
        match ext {
            "rs" => Some(Lang::Rust),
            "ts" | "tsx" | "js" | "mjs" | "cjs" => Some(Lang::TypeScript),
            "py" | "pyi" => Some(Lang::Python),
            "sh" | "yml" | "yaml" | "toml" => Some(Lang::Hash),
            _ => None,
        }
    }
}

/// One source line: its code (strings blanked) and its comment text, without
/// the comment markers.
#[derive(Debug, Default, Clone)]
pub struct Line {
    pub number: usize,
    pub code: String,
    pub comment: String,
}

pub fn split(src: &str, lang: Lang) -> Vec<Line> {
    match lang {
        Lang::Rust => CLike::new(true).run(src),
        Lang::TypeScript => CLike::new(false).run(src),
        Lang::Python => python(src),
        Lang::Hash => hash(src),
    }
}

// ---------------------------------------------------------------------------
// Rust and TypeScript
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Code,
    Block(u32),
    Str(char),
    RawStr(usize),
    Template,
}

struct CLike {
    rust: bool,
    state: State,
}

impl CLike {
    fn new(rust: bool) -> Self {
        Self {
            rust,
            state: State::Code,
        }
    }

    fn run(mut self, src: &str) -> Vec<Line> {
        src.lines()
            .enumerate()
            .map(|(i, raw)| self.line(i + 1, raw))
            .collect()
    }

    fn line(&mut self, number: usize, raw: &str) -> Line {
        let chars: Vec<char> = raw.chars().collect();
        let mut out = Line {
            number,
            ..Line::default()
        };
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            let next = chars.get(i + 1).copied();
            match self.state {
                State::Block(depth) => {
                    if c == '*' && next == Some('/') {
                        self.state = if depth > 1 {
                            State::Block(depth - 1)
                        } else {
                            State::Code
                        };
                        i += 2;
                    } else if self.rust && c == '/' && next == Some('*') {
                        self.state = State::Block(depth + 1);
                        i += 2;
                    } else {
                        out.comment.push(c);
                        i += 1;
                    }
                }
                State::Str(quote) => {
                    if c == '\\' {
                        out.code.push_str("  ");
                        i += 2;
                    } else {
                        if c == quote {
                            self.state = State::Code;
                            out.code.push(c);
                        } else {
                            out.code.push(' ');
                        }
                        i += 1;
                    }
                }
                State::RawStr(hashes) => {
                    if c == '"'
                        && chars[i + 1..]
                            .iter()
                            .take(hashes)
                            .filter(|&&h| h == '#')
                            .count()
                            == hashes
                    {
                        self.state = State::Code;
                        out.code.push('"');
                        i += 1 + hashes;
                    } else {
                        out.code.push(' ');
                        i += 1;
                    }
                }
                State::Template => {
                    if c == '\\' {
                        out.code.push_str("  ");
                        i += 2;
                    } else {
                        if c == '`' {
                            self.state = State::Code;
                            out.code.push(c);
                        } else {
                            out.code.push(' ');
                        }
                        i += 1;
                    }
                }
                State::Code => {
                    if c == '/' && next == Some('/') {
                        let text: String = chars[i + 2..].iter().collect();
                        // `///`, `//!` (doc comments) carry the same prose.
                        out.comment.push_str(text.trim_start_matches(['/', '!']));
                        break;
                    } else if c == '/' && next == Some('*') {
                        self.state = State::Block(1);
                        i += 2;
                    } else if c == '"' {
                        self.state = State::Str('"');
                        out.code.push(c);
                        i += 1;
                    } else if !self.rust && c == '\'' {
                        self.state = State::Str('\'');
                        out.code.push(c);
                        i += 1;
                    } else if !self.rust && c == '`' {
                        self.state = State::Template;
                        out.code.push(c);
                        i += 1;
                    } else if self.rust
                        && let Some(skip) = raw_string_start(&chars, i)
                    {
                        // r"..", r#".."#, br#".."#: the number of hashes closes it.
                        let hashes = skip - 2 - usize::from(chars[i] == 'b');
                        self.state = State::RawStr(hashes);
                        out.code.push('"');
                        i += skip;
                    } else if self.rust && c == '\'' {
                        i += rust_quote(&chars, i, &mut out.code);
                    } else {
                        out.code.push(c);
                        i += 1;
                    }
                }
            }
        }
        // A JS string ends with its line; an unclosed one (an apostrophe in JSX
        // text) must not swallow the lines after it.
        if !self.rust && matches!(self.state, State::Str(_)) {
            self.state = State::Code;
        }
        // A block comment's continuation lines often start with ` * `.
        if !out.comment.is_empty() {
            let trimmed = out.comment.trim_start();
            if let Some(rest) = trimmed.strip_prefix('*')
                && !rest.starts_with('/')
            {
                out.comment = rest.to_string();
            }
        }
        out
    }
}

/// Length of a raw string opener at `i` (`r"`, `r#"`, `br##"`), if one starts there.
fn raw_string_start(chars: &[char], i: usize) -> Option<usize> {
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    if i > 0 && ident(chars[i - 1]) {
        return None;
    }
    let mut j = i;
    if chars.get(j) == Some(&'b') {
        j += 1;
    }
    if chars.get(j) != Some(&'r') {
        return None;
    }
    j += 1;
    while chars.get(j) == Some(&'#') {
        j += 1;
    }
    (chars.get(j) == Some(&'"')).then_some(j + 1 - i)
}

/// A Rust `'`: a char literal (blanked) or a lifetime (kept). Returns how many
/// characters were consumed.
fn rust_quote(chars: &[char], i: usize, code: &mut String) -> usize {
    if chars.get(i + 1) == Some(&'\\') {
        // '\n', '\'', '\u{..}': up to the closing quote.
        let mut j = i + 2;
        while j < chars.len() && chars[j] != '\'' {
            j += 1;
        }
        code.push_str("' '");
        return (j + 1 - i).min(chars.len() - i);
    }
    if chars.get(i + 2) == Some(&'\'') {
        code.push_str("' '");
        return 3;
    }
    code.push('\'');
    1
}

// ---------------------------------------------------------------------------
// Python
// ---------------------------------------------------------------------------

fn python(src: &str) -> Vec<Line> {
    let mut out = Vec::new();
    // Inside a triple-quoted string: its quote, and whether it is a docstring
    // (prose, read as a comment) or data.
    let mut triple: Option<(char, bool)> = None;
    for (n, raw) in src.lines().enumerate() {
        let chars: Vec<char> = raw.chars().collect();
        let mut line = Line {
            number: n + 1,
            ..Line::default()
        };
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            if let Some((q, doc)) = triple {
                if c == '\\' {
                    i += 2;
                    continue;
                }
                if c == q && chars.get(i + 1) == Some(&q) && chars.get(i + 2) == Some(&q) {
                    triple = None;
                    line.code.push_str("\"\"\"");
                    i += 3;
                    continue;
                }
                if doc {
                    line.comment.push(c);
                } else {
                    line.code.push(' ');
                }
                i += 1;
                continue;
            }
            match c {
                '#' => {
                    line.comment.push_str(&raw[byte_index(raw, i + 1)..]);
                    break;
                }
                '\'' | '"' => {
                    if chars.get(i + 1) == Some(&c) && chars.get(i + 2) == Some(&c) {
                        // A string that opens a statement is a docstring.
                        let before = line.code.trim();
                        let prefix_only = before.chars().all(|p| "rRbBuUfF".contains(p));
                        triple = Some((c, prefix_only));
                        line.code.push_str("\"\"\"");
                        i += 3;
                    } else {
                        line.code.push(c);
                        i += 1;
                        while i < chars.len() && chars[i] != c {
                            if chars[i] == '\\' {
                                line.code.push(' ');
                                i += 1;
                            }
                            line.code.push(' ');
                            i += 1;
                        }
                        line.code.push(c);
                        i += 1;
                    }
                }
                _ => {
                    line.code.push(c);
                    i += 1;
                }
            }
        }
        out.push(line);
    }
    out
}

fn byte_index(s: &str, char_index: usize) -> usize {
    s.char_indices()
        .nth(char_index)
        .map(|(b, _)| b)
        .unwrap_or(s.len())
}

// ---------------------------------------------------------------------------
// Shell, YAML, TOML
// ---------------------------------------------------------------------------

fn hash(src: &str) -> Vec<Line> {
    src.lines()
        .enumerate()
        .map(|(n, raw)| {
            let mut line = Line {
                number: n + 1,
                ..Line::default()
            };
            let mut quote: Option<char> = None;
            let mut prev = ' ';
            for (b, c) in raw.char_indices() {
                match quote {
                    Some(q) if c == q => quote = None,
                    Some(_) => {}
                    None if c == '\'' || c == '"' => quote = Some(c),
                    // A comment starts a line or follows whitespace (a `#` in
                    // a URL or a ${#var} is not one).
                    None if c == '#' && prev.is_whitespace() => {
                        line.comment.push_str(&raw[b + 1..]);
                        break;
                    }
                    None => {}
                }
                line.code.push(c);
                prev = c;
            }
            line
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn comments(src: &str, lang: Lang) -> Vec<String> {
        split(src, lang)
            .into_iter()
            .map(|l| l.comment.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect()
    }

    fn code(src: &str, lang: Lang) -> String {
        split(src, lang)
            .into_iter()
            .map(|l| l.code)
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn rust_comments_strings_and_lifetimes() {
        let src = r##"/// Doc line
fn f<'a>(x: &'a str) -> char { // trailing
    let s = "// not a comment";
    let r = r#"/* still a string */"#;
    let c = '"'; /* block
       continues */ let d = '\'';
    'x'
}"##;
        assert_eq!(
            comments(src, Lang::Rust),
            ["Doc line", "trailing", "block", "continues"]
        );
        let code = code(src, Lang::Rust);
        assert!(code.contains("fn f<'a>(x: &'a str)"));
        assert!(!code.contains("not a comment"));
        assert!(!code.contains("still a string"));
        assert!(code.contains("let d ="));
    }

    #[test]
    fn nested_rust_block_comments() {
        let src = "/* outer /* inner */ still outer */ code();";
        assert_eq!(code(src, Lang::Rust).trim(), "code();");
    }

    #[test]
    fn typescript_quotes_and_templates() {
        let src = "const a = 'it\\'s // fine'; // real\nconst t = `x ${y} // no`; /** doc */";
        assert_eq!(comments(src, Lang::TypeScript), ["real", "doc"]);
    }

    #[test]
    fn python_docstrings_are_prose_and_data_strings_are_not() {
        let src = "def f():\n    \"\"\"Docstring here.\"\"\"\n    x = \"\"\"data\n# not a comment\n\"\"\"\n    y = '#'  # real\n";
        assert_eq!(comments(src, Lang::Python), ["Docstring here.", "real"]);
    }

    #[test]
    fn hash_comments_need_whitespace_before() {
        let src = "url: https://x.y/#frag # note\nkey = \"a # b\"\n# full line";
        assert_eq!(comments(src, Lang::Hash), ["note", "full line"]);
    }
}
