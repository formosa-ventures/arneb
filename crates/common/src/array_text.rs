//! PostgreSQL array text syntax (`{1,NULL,"a b"}`), the one parser behind
//! `'{...}'::int[]` casts and text-format array bind parameters.
//!
//! Only one-dimensional arrays are accepted. Rules follow PostgreSQL's
//! `array_in`: whitespace around an element is ignored, an unquoted `NULL`
//! (any case) is a NULL element, a quoted element keeps its text verbatim
//! (so `"NULL"` is the string), and a backslash escapes the next character
//! both inside and outside quotes.

/// Parses `text` into its elements, `None` for a NULL element.
///
/// Returns an error message (without a `malformed array literal` prefix) for
/// text that is not a one-dimensional PostgreSQL array literal.
pub fn parse_array_text(text: &str) -> Result<Vec<Option<String>>, String> {
    let malformed = |why: &str| format!("malformed array literal: \"{text}\" ({why})");
    let mut chars = text.trim().chars().peekable();
    if chars.next() != Some('{') {
        return Err(malformed("array value must start with \"{\""));
    }
    let skip_ws = |it: &mut std::iter::Peekable<std::str::Chars<'_>>| {
        while it.peek().is_some_and(|c| c.is_whitespace()) {
            it.next();
        }
    };
    let mut out = Vec::new();
    skip_ws(&mut chars);
    if chars.peek() == Some(&'}') {
        chars.next();
    } else {
        loop {
            skip_ws(&mut chars);
            match chars.peek() {
                Some('{') => return Err(malformed("multidimensional arrays are not supported")),
                Some('"') => {
                    chars.next();
                    let mut value = String::new();
                    loop {
                        match chars.next() {
                            Some('\\') => value.push(
                                chars
                                    .next()
                                    .ok_or_else(|| malformed("unterminated quoted element"))?,
                            ),
                            Some('"') => break,
                            Some(c) => value.push(c),
                            None => return Err(malformed("unterminated quoted element")),
                        }
                    }
                    out.push(Some(value));
                }
                _ => {
                    let mut value = String::new();
                    // Characters after a backslash are literal, so they also
                    // keep an element from being read as `NULL`.
                    let mut escaped = false;
                    while let Some(&c) = chars.peek() {
                        if c == ',' || c == '}' {
                            break;
                        }
                        chars.next();
                        match c {
                            '\\' => {
                                escaped = true;
                                value
                                    .push(chars.next().ok_or_else(|| malformed("unexpected end"))?);
                            }
                            '"' | '{' => return Err(malformed("unexpected character")),
                            _ => value.push(c),
                        }
                    }
                    let value = if escaped {
                        value
                    } else {
                        value.trim_end().to_string()
                    };
                    if value.is_empty() {
                        return Err(malformed("missing element"));
                    }
                    if !escaped && value.eq_ignore_ascii_case("null") {
                        out.push(None);
                    } else {
                        out.push(Some(value));
                    }
                }
            }
            skip_ws(&mut chars);
            match chars.next() {
                Some(',') => {}
                Some('}') => break,
                _ => return Err(malformed("expected \",\" or \"}\"")),
            }
        }
    }
    skip_ws(&mut chars);
    if chars.next().is_some() {
        return Err(malformed("junk after closing right brace"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn some(s: &str) -> Option<String> {
        Some(s.to_string())
    }

    #[test]
    fn parses_plain_null_and_whitespace() {
        assert_eq!(
            parse_array_text(" { 1 , NULL,null, 3 } ").unwrap(),
            [some("1"), None, None, some("3")]
        );
        assert_eq!(
            parse_array_text("{}").unwrap(),
            Vec::<Option<String>>::new()
        );
        assert_eq!(
            parse_array_text("{ }").unwrap(),
            Vec::<Option<String>>::new()
        );
    }

    #[test]
    fn parses_quotes_and_escapes() {
        assert_eq!(
            parse_array_text(r#"{"a b","","NULL","q\"x","b\\s","{c}",c\,d,\NULL}"#).unwrap(),
            [
                some("a b"),
                some(""),
                some("NULL"),
                some("q\"x"),
                some("b\\s"),
                some("{c}"),
                some("c,d"),
                some("NULL"),
            ]
        );
    }

    #[test]
    fn rejects_malformed_text() {
        for bad in [
            "",
            "1,2",
            "{1,2",
            "{1,,2}",
            "{,}",
            "{1,}",
            "{\"a}",
            "{1}x",
            "{{1},{2}}",
            "{a\"b}",
            "{a{b}",
        ] {
            assert!(parse_array_text(bad).is_err(), "{bad:?} should be refused");
        }
    }
}
