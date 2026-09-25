//! Lightweight recognizer for the Trino-only statements a client protocol
//! must answer itself: session control (`USE`, `SET SESSION`, …), prepared
//! statements (`PREPARE`, `EXECUTE`, `EXECUTE IMMEDIATE`, `DEALLOCATE`) and
//! the `SHOW` / `DESCRIBE` family BI tools use to browse metadata.
//!
//! Anything not recognized here goes to the regular SQL pipeline.

/// A lexical token of a control statement.
#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// Identifier or keyword; `quoted` is true for `"double quoted"` names.
    Ident { text: String, quoted: bool },
    /// Single-quoted string literal (unescaped).
    Str(String),
    /// Any other single character (`.`, `=`, `,`, `(`, …) or a number.
    Other(String),
}

fn lex(sql: &str) -> Option<Vec<Tok>> {
    let chars: Vec<char> = sql.chars().collect();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '\'' || c == '"' {
            let mut text = String::new();
            i += 1;
            loop {
                let ch = *chars.get(i)?;
                if ch == c {
                    if chars.get(i + 1) == Some(&c) {
                        text.push(c);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                text.push(ch);
                i += 1;
            }
            toks.push(if c == '\'' {
                Tok::Str(text)
            } else {
                Tok::Ident { text, quoted: true }
            });
        } else if c.is_alphanumeric() || c == '_' || c == '$' || c == '@' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_alphanumeric() || matches!(chars[i], '_' | '$' | '@'))
            {
                i += 1;
            }
            toks.push(Tok::Ident {
                text: chars[start..i].iter().collect(),
                quoted: false,
            });
        } else {
            toks.push(Tok::Other(c.to_string()));
            i += 1;
        }
    }
    Some(toks)
}

struct Cursor {
    toks: Vec<Tok>,
    pos: usize,
}

impl Cursor {
    fn keyword(&mut self, kw: &str) -> bool {
        match self.toks.get(self.pos) {
            Some(Tok::Ident {
                text,
                quoted: false,
            }) if text.eq_ignore_ascii_case(kw) => {
                self.pos += 1;
                true
            }
            _ => false,
        }
    }

    fn keywords(&mut self, kws: &[&str]) -> bool {
        let save = self.pos;
        for kw in kws {
            if !self.keyword(kw) {
                self.pos = save;
                return false;
            }
        }
        true
    }

    fn symbol(&mut self, s: &str) -> bool {
        match self.toks.get(self.pos) {
            Some(Tok::Other(t)) if t == s => {
                self.pos += 1;
                true
            }
            _ => false,
        }
    }

    fn ident(&mut self) -> Option<String> {
        match self.toks.get(self.pos) {
            Some(Tok::Ident { text, .. }) => {
                self.pos += 1;
                Some(text.clone())
            }
            _ => None,
        }
    }

    /// `a`, `a.b`, `a.b.c`, …
    fn qualified_name(&mut self) -> Option<Vec<String>> {
        let mut parts = vec![self.ident()?];
        while self.symbol(".") {
            parts.push(self.ident()?);
        }
        Some(parts)
    }

    fn string(&mut self) -> Option<String> {
        match self.toks.get(self.pos) {
            Some(Tok::Str(s)) => {
                self.pos += 1;
                Some(s.clone())
            }
            _ => None,
        }
    }

    fn at_end(&self) -> bool {
        self.pos >= self.toks.len()
    }

    /// Optional `LIKE 'pattern' [ESCAPE 'c']`.
    fn like(&mut self) -> Option<Option<String>> {
        if !self.keyword("LIKE") {
            return Some(None);
        }
        let pattern = self.string()?;
        if self.keyword("ESCAPE") {
            self.string()?;
        }
        Some(Some(pattern))
    }
}

/// A recognized Trino control / metadata statement.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ControlStatement {
    ShowCatalogs {
        like: Option<String>,
    },
    ShowSchemas {
        catalog: Option<String>,
        like: Option<String>,
    },
    ShowTables {
        schema: Option<Vec<String>>,
        like: Option<String>,
    },
    ShowColumns {
        table: Vec<String>,
    },
    ShowSession,
    Use {
        catalog: Option<String>,
        schema: String,
    },
    SetSession {
        name: String,
        value: String,
    },
    ResetSession {
        name: String,
    },
    /// `START TRANSACTION`, `COMMIT`, `ROLLBACK`, `SET TIME ZONE`, `SET ROLE`,
    /// `SET PATH`: accepted without effect. Carries the update type.
    NoOp(&'static str),
    Prepare {
        name: String,
        sql: String,
    },
    Deallocate {
        name: String,
    },
    Execute {
        name: String,
        params: Vec<String>,
    },
    ExecuteImmediate {
        sql: String,
        params: Vec<String>,
    },
}

/// Returns the substring of `sql` after the first `n` whitespace-separated
/// words (used to take the raw statement text of `PREPARE x FROM <sql>`).
fn after_words(sql: &str, n: usize) -> &str {
    let mut rest = sql.trim_start();
    for _ in 0..n {
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        rest = rest[end..].trim_start();
    }
    rest
}

/// Splits `USING a, 'b, c', f(x, y)` parameter lists on top-level commas.
pub(crate) fn split_params(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    let mut cur = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match quote {
            Some(q) => {
                cur.push(c);
                if c == q {
                    if chars.peek() == Some(&q) {
                        cur.push(chars.next().unwrap_or(q));
                    } else {
                        quote = None;
                    }
                }
            }
            None => match c {
                '\'' | '"' => {
                    quote = Some(c);
                    cur.push(c);
                }
                '(' | '[' => {
                    depth += 1;
                    cur.push(c);
                }
                ')' | ']' => {
                    depth -= 1;
                    cur.push(c);
                }
                ',' if depth == 0 => {
                    out.push(cur.trim().to_string());
                    cur.clear();
                }
                _ => cur.push(c),
            },
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

/// Substitutes `?` placeholders (outside string literals, quoted
/// identifiers and comments) with the given SQL expressions, in order.
pub(crate) fn bind_placeholders(sql: &str, params: &[String]) -> Result<String, String> {
    let mut out = String::with_capacity(sql.len());
    let mut next = 0usize;
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '\'' | '"' => {
                out.push(c);
                i += 1;
                while i < chars.len() {
                    out.push(chars[i]);
                    if chars[i] == c {
                        if chars.get(i + 1) == Some(&c) {
                            out.push(c);
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            '-' if chars.get(i + 1) == Some(&'-') => {
                while i < chars.len() && chars[i] != '\n' {
                    out.push(chars[i]);
                    i += 1;
                }
            }
            '?' => {
                let param = params.get(next).ok_or_else(|| {
                    format!(
                        "Incorrect number of parameters: expected at least {} but found {}",
                        next + 1,
                        params.len()
                    )
                })?;
                out.push_str(param);
                next += 1;
                i += 1;
            }
            _ => {
                out.push(c);
                i += 1;
            }
        }
    }
    if next != params.len() {
        return Err(format!(
            "Incorrect number of parameters: expected {next} but found {}",
            params.len()
        ));
    }
    Ok(out)
}

/// Splits `<name> [USING p1, p2]` / `'<sql>' [USING …]` tails.
fn using_params(rest: &str) -> Vec<String> {
    // Find a top-level USING keyword outside quotes.
    let lower = rest.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut quote: Option<u8> = None;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match quote {
            Some(q) if b == q => quote = None,
            Some(_) => {}
            None if b == b'\'' || b == b'"' => quote = Some(b),
            None if lower[i..].starts_with("using")
                && (i == 0 || bytes[i - 1].is_ascii_whitespace() || bytes[i - 1] == b'\'')
                && lower[i + 5..]
                    .chars()
                    .next()
                    .is_none_or(|c| c.is_whitespace()) =>
            {
                return split_params(&rest[i + 5..]);
            }
            None => {}
        }
        i += 1;
    }
    Vec::new()
}

/// Recognizes a Trino control statement. Returns `None` for ordinary SQL.
pub(crate) fn parse_control(sql: &str) -> Option<ControlStatement> {
    let toks = lex(sql)?;
    let mut c = Cursor { toks, pos: 0 };

    if c.keyword("SHOW") {
        if c.keyword("CATALOGS") {
            let like = c.like()?;
            return c
                .at_end()
                .then_some(ControlStatement::ShowCatalogs { like });
        }
        if c.keyword("SCHEMAS") {
            let catalog = if c.keyword("FROM") || c.keyword("IN") {
                Some(c.ident()?)
            } else {
                None
            };
            let like = c.like()?;
            return c
                .at_end()
                .then_some(ControlStatement::ShowSchemas { catalog, like });
        }
        if c.keyword("TABLES") {
            let schema = if c.keyword("FROM") || c.keyword("IN") {
                Some(c.qualified_name()?)
            } else {
                None
            };
            let like = c.like()?;
            return c
                .at_end()
                .then_some(ControlStatement::ShowTables { schema, like });
        }
        if c.keyword("COLUMNS") {
            if !(c.keyword("FROM") || c.keyword("IN")) {
                return None;
            }
            let table = c.qualified_name()?;
            return c
                .at_end()
                .then_some(ControlStatement::ShowColumns { table });
        }
        if c.keyword("SESSION") {
            c.like()?;
            return c.at_end().then_some(ControlStatement::ShowSession);
        }
        return None;
    }

    if c.keyword("DESCRIBE") || c.keyword("DESC") {
        if c.keyword("INPUT") || c.keyword("OUTPUT") {
            return None;
        }
        let table = c.qualified_name()?;
        return c
            .at_end()
            .then_some(ControlStatement::ShowColumns { table });
    }

    if c.keyword("USE") {
        let parts = c.qualified_name()?;
        if !c.at_end() {
            return None;
        }
        return match parts.as_slice() {
            [schema] => Some(ControlStatement::Use {
                catalog: None,
                schema: schema.clone(),
            }),
            [catalog, schema] => Some(ControlStatement::Use {
                catalog: Some(catalog.clone()),
                schema: schema.clone(),
            }),
            _ => None,
        };
    }

    if c.keywords(&["SET", "SESSION"]) {
        let name = c.qualified_name()?.join(".");
        if !c.symbol("=") {
            return None;
        }
        // Value: everything after the `=`; a single-quoted literal is
        // unquoted, anything else (number, boolean, identifier) is kept raw.
        let raw = sql.split_once('=')?.1.trim();
        let value = match raw.strip_prefix('\'').and_then(|r| r.strip_suffix('\'')) {
            Some(inner) => inner.replace("''", "'"),
            None => raw.to_string(),
        };
        if value.is_empty() {
            return None;
        }
        return Some(ControlStatement::SetSession { name, value });
    }
    if c.keywords(&["RESET", "SESSION"]) {
        let name = c.qualified_name()?.join(".");
        return c
            .at_end()
            .then_some(ControlStatement::ResetSession { name });
    }
    if c.keywords(&["SET", "TIME", "ZONE"]) {
        return Some(ControlStatement::NoOp("SET TIME ZONE"));
    }
    if c.keywords(&["SET", "ROLE"]) {
        return Some(ControlStatement::NoOp("SET ROLE"));
    }
    if c.keywords(&["SET", "PATH"]) {
        return Some(ControlStatement::NoOp("SET PATH"));
    }
    if c.keywords(&["START", "TRANSACTION"]) {
        return Some(ControlStatement::NoOp("START TRANSACTION"));
    }
    if c.keyword("COMMIT") {
        return Some(ControlStatement::NoOp("COMMIT"));
    }
    if c.keyword("ROLLBACK") {
        return Some(ControlStatement::NoOp("ROLLBACK"));
    }

    if c.keyword("PREPARE") {
        let name = c.ident()?;
        if !c.keyword("FROM") {
            return None;
        }
        let body = after_words(sql, 3).to_string();
        if body.is_empty() {
            return None;
        }
        return Some(ControlStatement::Prepare { name, sql: body });
    }
    if c.keywords(&["DEALLOCATE", "PREPARE"]) {
        let name = c.ident()?;
        return c.at_end().then_some(ControlStatement::Deallocate { name });
    }
    if c.keywords(&["EXECUTE", "IMMEDIATE"]) {
        let body = c.string()?;
        let rest = after_words(sql, 2);
        // Skip the literal itself before looking for USING.
        let params = using_params(rest);
        return Some(ControlStatement::ExecuteImmediate { sql: body, params });
    }
    if c.keyword("EXECUTE") {
        let name = c.ident()?;
        let params = using_params(after_words(sql, 2));
        return Some(ControlStatement::Execute { name, params });
    }
    None
}

/// SQL `LIKE` match (`%` = any run, `_` = any single character).
pub(crate) fn like_match(pattern: &str, value: &str) -> bool {
    fn rec(p: &[char], v: &[char]) -> bool {
        match p.first() {
            None => v.is_empty(),
            Some('%') => (0..=v.len()).any(|i| rec(&p[1..], &v[i..])),
            Some('_') => !v.is_empty() && rec(&p[1..], &v[1..]),
            Some(c) => v.first() == Some(c) && rec(&p[1..], &v[1..]),
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    rec(&p, &v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_show_family() {
        assert_eq!(
            parse_control("SHOW CATALOGS"),
            Some(ControlStatement::ShowCatalogs { like: None })
        );
        assert_eq!(
            parse_control("show schemas from datalake like 'tp%'"),
            Some(ControlStatement::ShowSchemas {
                catalog: Some("datalake".into()),
                like: Some("tp%".into())
            })
        );
        assert_eq!(
            parse_control("SHOW TABLES IN \"datalake\".tpch"),
            Some(ControlStatement::ShowTables {
                schema: Some(vec!["datalake".into(), "tpch".into()]),
                like: None
            })
        );
        assert_eq!(
            parse_control("DESCRIBE tpch.nation"),
            Some(ControlStatement::ShowColumns {
                table: vec!["tpch".into(), "nation".into()]
            })
        );
        assert_eq!(parse_control("SHOW CREATE TABLE t"), None);
        assert_eq!(parse_control("SELECT 1"), None);
    }

    #[test]
    fn recognizes_session_control() {
        assert_eq!(
            parse_control("USE datalake.tpch"),
            Some(ControlStatement::Use {
                catalog: Some("datalake".into()),
                schema: "tpch".into()
            })
        );
        assert_eq!(
            parse_control("SET SESSION query_max_run_time = '10m'"),
            Some(ControlStatement::SetSession {
                name: "query_max_run_time".into(),
                value: "10m".into()
            })
        );
        assert_eq!(
            parse_control("SET SESSION hive.bucket_execution_enabled = false"),
            Some(ControlStatement::SetSession {
                name: "hive.bucket_execution_enabled".into(),
                value: "false".into()
            })
        );
        assert_eq!(
            parse_control("SET SESSION ratio = 1.5"),
            Some(ControlStatement::SetSession {
                name: "ratio".into(),
                value: "1.5".into()
            })
        );
        assert_eq!(
            parse_control("RESET SESSION hive.x"),
            Some(ControlStatement::ResetSession {
                name: "hive.x".into()
            })
        );
        assert_eq!(
            parse_control("START TRANSACTION"),
            Some(ControlStatement::NoOp("START TRANSACTION"))
        );
    }

    #[test]
    fn recognizes_prepared_statements() {
        assert_eq!(
            parse_control("PREPARE q1 FROM SELECT * FROM t WHERE a = ?"),
            Some(ControlStatement::Prepare {
                name: "q1".into(),
                sql: "SELECT * FROM t WHERE a = ?".into()
            })
        );
        assert_eq!(
            parse_control("EXECUTE q1 USING 1, 'a, b'"),
            Some(ControlStatement::Execute {
                name: "q1".into(),
                params: vec!["1".into(), "'a, b'".into()]
            })
        );
        assert_eq!(
            parse_control("EXECUTE IMMEDIATE 'SELECT ? + ?, ''x''' USING 1, 2"),
            Some(ControlStatement::ExecuteImmediate {
                sql: "SELECT ? + ?, 'x'".into(),
                params: vec!["1".into(), "2".into()]
            })
        );
        assert_eq!(
            parse_control("DEALLOCATE PREPARE q1"),
            Some(ControlStatement::Deallocate { name: "q1".into() })
        );
    }

    #[test]
    fn binds_placeholders_outside_literals() {
        let sql = "SELECT '?' AS q, a FROM t WHERE b = ? AND c = ?";
        assert_eq!(
            bind_placeholders(sql, &["1".into(), "'x'".into()]).unwrap(),
            "SELECT '?' AS q, a FROM t WHERE b = 1 AND c = 'x'"
        );
        assert!(bind_placeholders(sql, &["1".into()]).is_err());
        assert!(bind_placeholders("SELECT 1", &["1".into()]).is_err());
    }

    #[test]
    fn like_patterns() {
        assert!(like_match("tp%", "tpch"));
        assert!(like_match("n_tion", "nation"));
        assert!(!like_match("tp%", "sales"));
        assert!(like_match("%", ""));
    }
}
