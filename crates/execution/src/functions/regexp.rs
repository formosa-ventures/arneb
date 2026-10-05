//! Regular-expression scalar functions (`regexp_like`, `regexp_extract`,
//! `regexp_replace`, `regexp_count`).
//!
//! # Dialect
//!
//! Trino evaluates patterns with a Java-compatible engine; arneb uses
//! the Rust [`regex`] crate. The two agree on the common subset
//! (character classes, quantifiers, anchors, groups, named groups,
//! `(?i)`-style inline flags, Unicode classes). Differences to be aware
//! of:
//!
//! - Look-around (`(?=…)`, `(?<=…)`) and back-references (`\1`) are not
//!   supported and produce a "invalid regular expression" error.
//! - Matching is guaranteed linear-time (no catastrophic backtracking).
//!
//! Replacement strings use Java/Trino syntax — `$1`, `${name}`, and
//! `\$` for a literal dollar sign — and are translated to the `regex`
//! crate's syntax before use.

use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::types::DataType;
use arrow::array::{Array, ArrayRef, BooleanBuilder, Int64Builder, StringArray, StringBuilder};
use regex::Regex;

use super::args::{check_arity, constant_str, int64, invalid, utf8};
use super::registry::ScalarFunction;

/// Return all built-in regular-expression functions.
pub(crate) fn all_regexp_functions() -> Vec<Arc<dyn ScalarFunction>> {
    vec![
        Arc::new(RegexpLikeFunction),
        Arc::new(RegexpExtractFunction),
        Arc::new(RegexpReplaceFunction),
        Arc::new(RegexpCountFunction),
    ]
}

/// Compiles patterns on demand, reusing the last compiled regex while
/// consecutive rows share a pattern (the literal-pattern case compiles
/// exactly once per batch).
#[derive(Default)]
struct RegexCache {
    last: Option<(String, Regex)>,
}

impl RegexCache {
    fn get(&mut self, pattern: &str, fn_name: &str) -> Result<&Regex, ExecutionError> {
        let hit = matches!(&self.last, Some((p, _)) if p == pattern);
        if !hit {
            let re = Regex::new(pattern).map_err(|e| {
                invalid(format!(
                    "{fn_name}: invalid regular expression '{pattern}': {e}"
                ))
            })?;
            self.last = Some((pattern.to_string(), re));
        }
        Ok(&self.last.as_ref().expect("populated above").1)
    }
}

/// Translate a Java/Trino replacement string (`$1`, `${name}`, `\$`)
/// into `regex` crate syntax (`${1}`, `${name}`, `$$`).
fn translate_replacement(java: &str) -> String {
    let mut out = String::with_capacity(java.len() + 4);
    let mut chars = java.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some('$') => out.push_str("$$"),
                Some(other) => out.push(other),
                None => out.push('\\'),
            },
            '$' => match chars.peek() {
                Some('{') => {
                    out.push('$');
                    for c in chars.by_ref() {
                        out.push(c);
                        if c == '}' {
                            break;
                        }
                    }
                }
                Some(d) if d.is_ascii_digit() => {
                    out.push_str("${");
                    while let Some(d) = chars.peek().copied().filter(char::is_ascii_digit) {
                        out.push(d);
                        chars.next();
                    }
                    out.push('}');
                }
                _ => out.push_str("$$"),
            },
            other => out.push(other),
        }
    }
    out
}

// -- REGEXP_LIKE --

/// `regexp_like(string, pattern) -> boolean`: true if `pattern` matches
/// anywhere in `string` (use `^…$` to anchor). A constant pattern runs
/// through Arrow's vectorised `regexp_is_match_scalar` kernel.
#[derive(Debug)]
struct RegexpLikeFunction;

impl ScalarFunction for RegexpLikeFunction {
    fn name(&self) -> &str {
        "REGEXP_LIKE"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Boolean)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("REGEXP_LIKE", args, 2, 2)?;
        let s = utf8(&args[0], "REGEXP_LIKE")?;
        let pattern = utf8(&args[1], "REGEXP_LIKE")?;
        if let Some(p) = constant_str(&pattern) {
            // Validate up-front so a bad pattern reports a clean error.
            Regex::new(p).map_err(|e| {
                invalid(format!(
                    "REGEXP_LIKE: invalid regular expression '{p}': {e}"
                ))
            })?;
            let out = arrow::compute::kernels::regexp::regexp_is_match_scalar(&s, p, None)?;
            return Ok(Arc::new(out));
        }
        let mut cache = RegexCache::default();
        let mut out = BooleanBuilder::with_capacity(s.len());
        for i in 0..s.len() {
            if s.is_null(i) || pattern.is_null(i) {
                out.append_null();
                continue;
            }
            let re = cache.get(pattern.value(i), "REGEXP_LIKE")?;
            out.append_value(re.is_match(s.value(i)));
        }
        Ok(Arc::new(out.finish()))
    }
}

// -- REGEXP_EXTRACT --

/// `regexp_extract(string, pattern [, group]) -> varchar`: the first
/// substring matched by `pattern` (or its capturing `group`, 0 = whole
/// match). NULL when there is no match or the group did not
/// participate. A group number beyond the pattern's group count is an
/// error, as in Trino.
#[derive(Debug)]
struct RegexpExtractFunction;

impl ScalarFunction for RegexpExtractFunction {
    fn name(&self) -> &str {
        "REGEXP_EXTRACT"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("REGEXP_EXTRACT", args, 2, 3)?;
        let s = utf8(&args[0], "REGEXP_EXTRACT")?;
        let pattern = utf8(&args[1], "REGEXP_EXTRACT")?;
        let group = if args.len() == 3 {
            Some(int64(&args[2], "REGEXP_EXTRACT")?)
        } else {
            None
        };
        let mut cache = RegexCache::default();
        let mut out = StringBuilder::with_capacity(s.len(), 0);
        for i in 0..s.len() {
            if s.is_null(i) || pattern.is_null(i) || group.as_ref().is_some_and(|g| g.is_null(i)) {
                out.append_null();
                continue;
            }
            let re = cache.get(pattern.value(i), "REGEXP_EXTRACT")?;
            let g = group.as_ref().map_or(0, |g| g.value(i));
            let groups = re.captures_len() as i64 - 1;
            if g < 0 || g > groups {
                return Err(invalid(format!(
                    "REGEXP_EXTRACT: pattern has {groups} groups. Cannot access group {g}"
                )));
            }
            let m = re
                .captures(s.value(i))
                .and_then(|c| c.get(g as usize))
                .map(|m| m.as_str());
            out.append_option(m);
        }
        Ok(Arc::new(out.finish()))
    }
}

// -- REGEXP_REPLACE --

/// `regexp_replace(string, pattern [, replacement]) -> varchar`:
/// replaces every match of `pattern` with `replacement` (default: the
/// empty string, i.e. removes matches). `replacement` may reference
/// groups as `$1` or `${name}`; `\$` is a literal dollar sign.
#[derive(Debug)]
struct RegexpReplaceFunction;

impl ScalarFunction for RegexpReplaceFunction {
    fn name(&self) -> &str {
        "REGEXP_REPLACE"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("REGEXP_REPLACE", args, 2, 3)?;
        let s = utf8(&args[0], "REGEXP_REPLACE")?;
        let pattern = utf8(&args[1], "REGEXP_REPLACE")?;
        let replacement = if args.len() == 3 {
            utf8(&args[2], "REGEXP_REPLACE")?
        } else {
            StringArray::from(vec![""; s.len()])
        };
        let mut cache = RegexCache::default();
        let mut last_repl: Option<(String, String)> = None;
        let mut out = StringBuilder::with_capacity(s.len(), s.value_data().len());
        for i in 0..s.len() {
            if s.is_null(i) || pattern.is_null(i) || replacement.is_null(i) {
                out.append_null();
                continue;
            }
            let java = replacement.value(i);
            if !matches!(&last_repl, Some((j, _)) if j == java) {
                last_repl = Some((java.to_string(), translate_replacement(java)));
            }
            let repl = &last_repl.as_ref().expect("populated above").1;
            let re = cache.get(pattern.value(i), "REGEXP_REPLACE")?;
            out.append_value(re.replace_all(s.value(i), repl.as_str()));
        }
        Ok(Arc::new(out.finish()))
    }
}

// -- REGEXP_COUNT --

/// `regexp_count(string, pattern) -> bigint`: the number of
/// (non-overlapping) matches of `pattern` in `string`.
#[derive(Debug)]
struct RegexpCountFunction;

impl ScalarFunction for RegexpCountFunction {
    fn name(&self) -> &str {
        "REGEXP_COUNT"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Int64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("REGEXP_COUNT", args, 2, 2)?;
        let s = utf8(&args[0], "REGEXP_COUNT")?;
        let pattern = utf8(&args[1], "REGEXP_COUNT")?;
        let mut cache = RegexCache::default();
        let mut out = Int64Builder::with_capacity(s.len());
        for i in 0..s.len() {
            if s.is_null(i) || pattern.is_null(i) {
                out.append_null();
                continue;
            }
            let re = cache.get(pattern.value(i), "REGEXP_COUNT")?;
            out.append_value(re.find_iter(s.value(i)).count() as i64);
        }
        Ok(Arc::new(out.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int64Array, NullArray};

    fn strs(v: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(v.to_vec()))
    }

    #[test]
    fn regexp_like_constant_and_per_row_patterns() {
        let f = RegexpLikeFunction;
        let out = f
            .evaluate(&[
                strs(&[Some("abc123"), Some("xyz"), None]),
                strs(&[Some("\\d+"); 3]),
            ])
            .unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
        assert!(!out.value(1));
        assert!(out.is_null(2));

        let out = f
            .evaluate(&[
                strs(&[Some("abc"), Some("abc")]),
                strs(&[Some("^a"), Some("^b")]),
            ])
            .unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
        assert!(!out.value(1));
    }

    #[test]
    fn regexp_like_invalid_pattern_errors() {
        let f = RegexpLikeFunction;
        assert!(f
            .evaluate(&[strs(&[Some("a")]), strs(&[Some("(")])])
            .is_err());
    }

    #[test]
    fn regexp_extract_whole_match_and_group() {
        let f = RegexpExtractFunction;
        let out = f
            .evaluate(&[
                strs(&[Some("1a 2b 14m"), Some("none"), None]),
                strs(&[Some("\\d+"); 3]),
            ])
            .unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "1");
        assert!(out.is_null(1), "no match -> NULL");
        assert!(out.is_null(2));

        let group: ArrayRef = Arc::new(Int64Array::from(vec![2]));
        let out = f
            .evaluate(&[
                strs(&[Some("1a 2b 14m")]),
                strs(&[Some("(\\d+)([a-z]+)")]),
                group,
            ])
            .unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "a");
    }

    #[test]
    fn regexp_extract_group_out_of_range_errors() {
        let f = RegexpExtractFunction;
        let group: ArrayRef = Arc::new(Int64Array::from(vec![3]));
        assert!(f
            .evaluate(&[strs(&[Some("ab")]), strs(&[Some("(a)")]), group])
            .is_err());
    }

    #[test]
    fn regexp_replace_with_groups_and_removal() {
        let f = RegexpReplaceFunction;
        let out = f
            .evaluate(&[
                strs(&[Some("1a 2b 14m")]),
                strs(&[Some("(\\d+)([ab]) ")]),
                strs(&[Some("3c$2 ")]),
            ])
            .unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "3ca 3cb 14m");

        let out = f
            .evaluate(&[strs(&[Some("1a 2b 14m")]), strs(&[Some("\\d+[ab] ")])])
            .unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "14m");
    }

    #[test]
    fn replacement_translation() {
        assert_eq!(translate_replacement("$1x"), "${1}x");
        assert_eq!(translate_replacement("${name}!"), "${name}!");
        assert_eq!(translate_replacement("\\$5"), "$$5");
        assert_eq!(translate_replacement("a$"), "a$$");
    }

    #[test]
    fn regexp_count_counts_matches() {
        let f = RegexpCountFunction;
        let out = f
            .evaluate(&[
                strs(&[Some("1a 2b 14m"), Some("")]),
                strs(&[Some("\\d+"); 2]),
            ])
            .unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 3);
        assert_eq!(out.value(1), 0);
    }

    #[test]
    fn null_literal_pattern_yields_null() {
        let f = RegexpLikeFunction;
        let out = f
            .evaluate(&[strs(&[Some("a")]), Arc::new(NullArray::new(1))])
            .unwrap();
        assert!(out.is_null(0));
    }
}
