//! Built-in string scalar functions.

use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::types::DataType;
use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, Int64Builder, StringArray, StringBuilder,
};

use super::args::{check_arity, int64, invalid, utf8};
use super::registry::ScalarFunction;

/// Return all built-in string functions.
pub(crate) fn all_string_functions() -> Vec<Arc<dyn ScalarFunction>> {
    vec![
        Arc::new(UpperFunction),
        Arc::new(LowerFunction),
        Arc::new(SubstringFunction),
        Arc::new(TrimFunction),
        Arc::new(LtrimFunction),
        Arc::new(RtrimFunction),
        Arc::new(ConcatFunction),
        Arc::new(LengthFunction),
        Arc::new(ReplaceFunction),
        Arc::new(PositionFunction),
        // Trino batch 1 (trino-functions-batch1)
        Arc::new(SplitPartFunction),
        Arc::new(StartsWithFunction),
        Arc::new(StrposFunction),
        Arc::new(ReverseFunction),
        Arc::new(PadFunction { left: true }),
        Arc::new(PadFunction { left: false }),
        Arc::new(ChrFunction),
        Arc::new(CodepointFunction),
        Arc::new(ConcatWsFunction),
        Arc::new(LevenshteinFunction),
        Arc::new(HammingFunction),
        Arc::new(TranslateFunction),
    ]
}

fn require_string_arg(args: &[ArrayRef], idx: usize, fn_name: &str) -> Result<(), ExecutionError> {
    if idx >= args.len() {
        return Err(ExecutionError::InvalidOperation(format!(
            "{fn_name}: missing argument {idx}"
        )));
    }
    Ok(())
}

// -- UPPER --

#[derive(Debug)]
struct UpperFunction;

impl ScalarFunction for UpperFunction {
    fn name(&self) -> &str {
        "UPPER"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        require_string_arg(args, 0, "UPPER")?;
        let arr = &utf8(&args[0], self.name())?;
        let result: StringArray = arr.iter().map(|v| v.map(|s| s.to_uppercase())).collect();
        Ok(Arc::new(result))
    }
}

// -- LOWER --

#[derive(Debug)]
struct LowerFunction;

impl ScalarFunction for LowerFunction {
    fn name(&self) -> &str {
        "LOWER"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        require_string_arg(args, 0, "LOWER")?;
        let arr = &utf8(&args[0], self.name())?;
        let result: StringArray = arr.iter().map(|v| v.map(|s| s.to_lowercase())).collect();
        Ok(Arc::new(result))
    }
}

// -- SUBSTRING --

#[derive(Debug)]
struct SubstringFunction;

impl ScalarFunction for SubstringFunction {
    fn name(&self) -> &str {
        "SUBSTRING"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        // SUBSTRING(str, start [, length])
        if args.len() < 2 {
            return Err(ExecutionError::InvalidOperation(
                "SUBSTRING requires at least 2 arguments".to_string(),
            ));
        }
        let str_arr = &utf8(&args[0], self.name())?;
        let start_arr = &int64(&args[1], "SUBSTRING")?;
        let len_owned = if args.len() > 2 {
            Some(int64(&args[2], "SUBSTRING")?)
        } else {
            None
        };
        let len_arr = len_owned.as_ref();

        let result: StringArray = (0..str_arr.len())
            .map(|i| {
                if str_arr.is_null(i) || start_arr.is_null(i) {
                    return None;
                }
                let s = str_arr.value(i);
                // SQL SUBSTRING is 1-based
                let start = (start_arr.value(i) - 1).max(0) as usize;
                let chars: Vec<char> = s.chars().collect();
                if start >= chars.len() {
                    return Some(String::new());
                }
                let end = if let Some(la) = len_arr {
                    if la.is_null(i) {
                        return None;
                    }
                    (start + la.value(i).max(0) as usize).min(chars.len())
                } else {
                    chars.len()
                };
                Some(chars[start..end].iter().collect())
            })
            .collect();
        Ok(Arc::new(result))
    }
}

// -- TRIM / LTRIM / RTRIM --

#[derive(Debug)]
struct TrimFunction;

impl ScalarFunction for TrimFunction {
    fn name(&self) -> &str {
        "TRIM"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("TRIM", args, 1, 2)?;
        let arr = &utf8(&args[0], self.name())?;
        if args.len() == 1 {
            let result: StringArray = arr.iter().map(|v| v.map(|s| s.trim())).collect();
            return Ok(Arc::new(result));
        }
        let chars = utf8(&args[1], self.name())?;
        let result: StringArray = arr
            .iter()
            .zip(chars.iter())
            .map(|(v, c)| match (v, c) {
                (Some(s), Some(c)) => {
                    let set: Vec<char> = c.chars().collect();
                    Some(s.trim_matches(set.as_slice()))
                }
                _ => None,
            })
            .collect();
        Ok(Arc::new(result))
    }
}

#[derive(Debug)]
struct LtrimFunction;

impl ScalarFunction for LtrimFunction {
    fn name(&self) -> &str {
        "LTRIM"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("LTRIM", args, 1, 2)?;
        let arr = &utf8(&args[0], self.name())?;
        if args.len() == 1 {
            let result: StringArray = arr.iter().map(|v| v.map(|s| s.trim_start())).collect();
            return Ok(Arc::new(result));
        }
        let chars = utf8(&args[1], self.name())?;
        let result: StringArray = arr
            .iter()
            .zip(chars.iter())
            .map(|(v, c)| match (v, c) {
                (Some(s), Some(c)) => {
                    let set: Vec<char> = c.chars().collect();
                    Some(s.trim_start_matches(set.as_slice()))
                }
                _ => None,
            })
            .collect();
        Ok(Arc::new(result))
    }
}

#[derive(Debug)]
struct RtrimFunction;

impl ScalarFunction for RtrimFunction {
    fn name(&self) -> &str {
        "RTRIM"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("RTRIM", args, 1, 2)?;
        let arr = &utf8(&args[0], self.name())?;
        if args.len() == 1 {
            let result: StringArray = arr.iter().map(|v| v.map(|s| s.trim_end())).collect();
            return Ok(Arc::new(result));
        }
        let chars = utf8(&args[1], self.name())?;
        let result: StringArray = arr
            .iter()
            .zip(chars.iter())
            .map(|(v, c)| match (v, c) {
                (Some(s), Some(c)) => {
                    let set: Vec<char> = c.chars().collect();
                    Some(s.trim_end_matches(set.as_slice()))
                }
                _ => None,
            })
            .collect();
        Ok(Arc::new(result))
    }
}

// -- CONCAT --

#[derive(Debug)]
struct ConcatFunction;

impl ScalarFunction for ConcatFunction {
    fn name(&self) -> &str {
        "CONCAT"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        if args.is_empty() {
            return Err(ExecutionError::InvalidOperation(
                "CONCAT requires at least 1 argument".to_string(),
            ));
        }
        let len = args[0].len();
        let owned: Vec<StringArray> = args
            .iter()
            .map(|a| utf8(a, "CONCAT"))
            .collect::<Result<_, _>>()?;
        let str_arrays: Vec<&StringArray> = owned.iter().collect();

        let result: StringArray = (0..len)
            .map(|i| {
                let mut buf = String::new();
                let mut any_null = false;
                for arr in &str_arrays {
                    if arr.is_null(i) {
                        any_null = true;
                        break;
                    }
                    buf.push_str(arr.value(i));
                }
                if any_null {
                    None
                } else {
                    Some(buf)
                }
            })
            .collect();
        Ok(Arc::new(result))
    }
}

// -- LENGTH --

#[derive(Debug)]
struct LengthFunction;

impl ScalarFunction for LengthFunction {
    fn name(&self) -> &str {
        "LENGTH"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Int64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        require_string_arg(args, 0, "LENGTH")?;
        let arr = &utf8(&args[0], self.name())?;
        let result: Int64Array = arr
            .iter()
            .map(|v| v.map(|s| s.chars().count() as i64))
            .collect();
        Ok(Arc::new(result))
    }
}

// -- REPLACE --

#[derive(Debug)]
struct ReplaceFunction;

impl ScalarFunction for ReplaceFunction {
    fn name(&self) -> &str {
        "REPLACE"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        // REPLACE(str, search [, replacement]) — the 2-argument form
        // removes every occurrence of `search` (Trino).
        check_arity("REPLACE", args, 2, 3)?;
        let str_arr = &utf8(&args[0], self.name())?;
        let from_arr = &utf8(&args[1], self.name())?;
        let to_arr = &if args.len() == 3 {
            utf8(&args[2], self.name())?
        } else {
            StringArray::from(vec![""; str_arr.len()])
        };
        let result: StringArray = (0..str_arr.len())
            .map(|i| {
                if str_arr.is_null(i) || from_arr.is_null(i) || to_arr.is_null(i) {
                    None
                } else {
                    Some(str_arr.value(i).replace(from_arr.value(i), to_arr.value(i)))
                }
            })
            .collect();
        Ok(Arc::new(result))
    }
}

// -- POSITION --

#[derive(Debug)]
struct PositionFunction;

impl ScalarFunction for PositionFunction {
    fn name(&self) -> &str {
        "POSITION"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Int64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        // POSITION(substr, str) — returns 1-based position or 0 if not found
        if args.len() < 2 {
            return Err(ExecutionError::InvalidOperation(
                "POSITION requires 2 arguments".to_string(),
            ));
        }
        let substr_arr = &utf8(&args[0], self.name())?;
        let str_arr = &utf8(&args[1], self.name())?;
        let result: Int64Array = (0..str_arr.len())
            .map(|i| {
                if str_arr.is_null(i) || substr_arr.is_null(i) {
                    None
                } else {
                    let s = str_arr.value(i);
                    let sub = substr_arr.value(i);
                    Some(match s.find(sub) {
                        Some(pos) => {
                            // Convert byte offset to char position (1-based)
                            s[..pos].chars().count() as i64 + 1
                        }
                        None => 0,
                    })
                }
            })
            .collect();
        Ok(Arc::new(result))
    }
}

// ===========================================================================
// Trino batch 1 (trino-functions-batch1)
// ===========================================================================

/// 1-based character index of the byte offset `byte_pos` in `s`.
fn char_pos(s: &str, byte_pos: usize) -> i64 {
    s[..byte_pos].chars().count() as i64 + 1
}

// -- SPLIT_PART --

/// `split_part(string, delimiter, index) -> varchar`.
///
/// Splits `string` on `delimiter` and returns field `index` (1-based).
/// Returns NULL when `index` is past the last field; `index <= 0` is an
/// error (Trino: "Index must be greater than zero"). An empty delimiter
/// splits the string into single characters, like Trino.
#[derive(Debug)]
struct SplitPartFunction;

impl ScalarFunction for SplitPartFunction {
    fn name(&self) -> &str {
        "SPLIT_PART"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("SPLIT_PART", args, 3, 3)?;
        let s = utf8(&args[0], "SPLIT_PART")?;
        let delim = utf8(&args[1], "SPLIT_PART")?;
        let index = int64(&args[2], "SPLIT_PART")?;
        let mut out = StringBuilder::with_capacity(s.len(), s.value_data().len());
        for i in 0..s.len() {
            if s.is_null(i) || delim.is_null(i) || index.is_null(i) {
                out.append_null();
                continue;
            }
            let idx = index.value(i);
            if idx <= 0 {
                return Err(invalid("SPLIT_PART: index must be greater than zero"));
            }
            let (text, d) = (s.value(i), delim.value(i));
            let field = if d.is_empty() {
                text.char_indices()
                    .nth((idx - 1) as usize)
                    .map(|(b, c)| &text[b..b + c.len_utf8()])
            } else {
                text.split(d).nth((idx - 1) as usize)
            };
            out.append_option(field);
        }
        Ok(Arc::new(out.finish()))
    }
}

// -- STARTS_WITH --

/// `starts_with(string, prefix) -> boolean`, via Arrow's vectorised
/// `starts_with` kernel. NULL in either argument → NULL.
#[derive(Debug)]
struct StartsWithFunction;

impl ScalarFunction for StartsWithFunction {
    fn name(&self) -> &str {
        "STARTS_WITH"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Boolean)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("STARTS_WITH", args, 2, 2)?;
        let s = utf8(&args[0], "STARTS_WITH")?;
        let prefix = utf8(&args[1], "STARTS_WITH")?;
        Ok(Arc::new(arrow::compute::kernels::comparison::starts_with(
            &s, &prefix,
        )?))
    }
}

// -- STRPOS --

/// `strpos(string, substring [, instance]) -> bigint`.
///
/// 1-based character position of the `instance`-th (default first)
/// occurrence of `substring`, or 0 when not found. An empty substring
/// matches at position 1. `instance <= 0` is an error, as in Trino.
/// Occurrences may overlap (`strpos('aaa', 'aa', 2) = 2`), matching
/// Trino's implementation which restarts the search one character after
/// the previous match.
#[derive(Debug)]
struct StrposFunction;

impl ScalarFunction for StrposFunction {
    fn name(&self) -> &str {
        "STRPOS"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Int64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("STRPOS", args, 2, 3)?;
        let s = utf8(&args[0], "STRPOS")?;
        let sub = utf8(&args[1], "STRPOS")?;
        let instance = if args.len() == 3 {
            Some(int64(&args[2], "STRPOS")?)
        } else {
            None
        };
        let mut out = Int64Builder::with_capacity(s.len());
        for i in 0..s.len() {
            if s.is_null(i) || sub.is_null(i) || instance.as_ref().is_some_and(|a| a.is_null(i)) {
                out.append_null();
                continue;
            }
            let n = instance.as_ref().map_or(1, |a| a.value(i));
            if n <= 0 {
                return Err(invalid("STRPOS: 'instance' must be a positive number"));
            }
            let (text, needle) = (s.value(i), sub.value(i));
            if needle.is_empty() {
                out.append_value(1);
                continue;
            }
            let mut from = 0usize;
            let mut found = None;
            for _ in 0..n {
                match text[from..].find(needle) {
                    Some(off) => {
                        let at = from + off;
                        found = Some(at);
                        // Next search starts one character after this match.
                        from = at + text[at..].chars().next().map_or(1, char::len_utf8);
                    }
                    None => {
                        found = None;
                        break;
                    }
                }
            }
            out.append_value(found.map_or(0, |b| char_pos(text, b)));
        }
        Ok(Arc::new(out.finish()))
    }
}

// -- REVERSE --

/// `reverse(string) -> varchar`: the characters (code points) of
/// `string` in reverse order.
#[derive(Debug)]
struct ReverseFunction;

impl ScalarFunction for ReverseFunction {
    fn name(&self) -> &str {
        "REVERSE"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("REVERSE", args, 1, 1)?;
        let s = utf8(&args[0], "REVERSE")?;
        let result: StringArray = s
            .iter()
            .map(|v| v.map(|t| t.chars().rev().collect::<String>()))
            .collect();
        Ok(Arc::new(result))
    }
}

// -- LPAD / RPAD --

/// `lpad(string, size, padstring)` / `rpad(string, size, padstring)`.
///
/// Pads `string` on the left (right) to `size` characters with
/// `padstring` (repeated as needed). If `size` is less than the length
/// of `string`, the result is truncated to `size` characters. `size`
/// must be non-negative and `padstring` non-empty (errors otherwise,
/// as in Trino).
#[derive(Debug)]
struct PadFunction {
    left: bool,
}

impl ScalarFunction for PadFunction {
    fn name(&self) -> &str {
        if self.left {
            "LPAD"
        } else {
            "RPAD"
        }
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        let name = self.name();
        check_arity(name, args, 3, 3)?;
        let s = utf8(&args[0], name)?;
        let size = int64(&args[1], name)?;
        let pad = utf8(&args[2], name)?;
        let mut out = StringBuilder::with_capacity(s.len(), s.value_data().len());
        for i in 0..s.len() {
            if s.is_null(i) || size.is_null(i) || pad.is_null(i) {
                out.append_null();
                continue;
            }
            let target = size.value(i);
            if target < 0 {
                return Err(invalid(format!("{name}: target size must be non-negative")));
            }
            let target = target as usize;
            let (text, p) = (s.value(i), pad.value(i));
            if p.is_empty() {
                return Err(invalid(format!("{name}: padding string must not be empty")));
            }
            let len = text.chars().count();
            if len >= target {
                let cut: String = text.chars().take(target).collect();
                out.append_value(cut);
                continue;
            }
            let filler: String = p.chars().cycle().take(target - len).collect();
            if self.left {
                out.append_value(format!("{filler}{text}"));
            } else {
                out.append_value(format!("{text}{filler}"));
            }
        }
        Ok(Arc::new(out.finish()))
    }
}

// -- CHR / CODEPOINT --

/// `chr(n) -> varchar`: the character with Unicode code point `n`.
/// Invalid code points are an error, as in Trino.
#[derive(Debug)]
struct ChrFunction;

impl ScalarFunction for ChrFunction {
    fn name(&self) -> &str {
        "CHR"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("CHR", args, 1, 1)?;
        let n = int64(&args[0], "CHR")?;
        let mut out = StringBuilder::with_capacity(n.len(), n.len());
        for v in n.iter() {
            match v {
                None => out.append_null(),
                Some(cp) => {
                    let c = u32::try_from(cp)
                        .ok()
                        .and_then(char::from_u32)
                        .ok_or_else(|| {
                            invalid(format!("CHR: not a valid Unicode code point: {cp}"))
                        })?;
                    out.append_value(c.encode_utf8(&mut [0u8; 4]));
                }
            }
        }
        Ok(Arc::new(out.finish()))
    }
}

/// `codepoint(c) -> integer`: the Unicode code point of the single
/// character `c`. A string that is not exactly one character is an
/// error (Trino only accepts `varchar(1)`).
#[derive(Debug)]
struct CodepointFunction;

impl ScalarFunction for CodepointFunction {
    fn name(&self) -> &str {
        "CODEPOINT"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Int32)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("CODEPOINT", args, 1, 1)?;
        let s = utf8(&args[0], "CODEPOINT")?;
        let out: Int32Array = s
            .iter()
            .map(|v| {
                v.map(|t| {
                    let mut chars = t.chars();
                    match (chars.next(), chars.next()) {
                        (Some(c), None) => Ok(c as i32),
                        _ => Err(invalid(format!(
                            "CODEPOINT: argument must be a single character, got '{t}'"
                        ))),
                    }
                })
                .transpose()
            })
            .collect::<Result<_, _>>()?;
        Ok(Arc::new(out))
    }
}

// -- CONCAT_WS --

/// `concat_ws(separator, string1, ..., stringN) -> varchar`.
///
/// Joins the non-NULL strings with `separator`. NULL arguments are
/// skipped; a NULL separator yields NULL (Trino semantics). With only a
/// separator the result is the empty string.
#[derive(Debug)]
struct ConcatWsFunction;

impl ScalarFunction for ConcatWsFunction {
    fn name(&self) -> &str {
        "CONCAT_WS"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("CONCAT_WS", args, 1, usize::MAX)?;
        let sep = utf8(&args[0], "CONCAT_WS")?;
        let parts: Vec<StringArray> = args[1..]
            .iter()
            .map(|a| utf8(a, "CONCAT_WS"))
            .collect::<Result<_, _>>()?;
        let mut out = StringBuilder::with_capacity(sep.len(), 0);
        let mut buf = String::new();
        for i in 0..sep.len() {
            if sep.is_null(i) {
                out.append_null();
                continue;
            }
            buf.clear();
            let mut first = true;
            for p in parts.iter().filter(|p| !p.is_null(i)) {
                if !first {
                    buf.push_str(sep.value(i));
                }
                buf.push_str(p.value(i));
                first = false;
            }
            out.append_value(&buf);
        }
        Ok(Arc::new(out.finish()))
    }
}

// -- LEVENSHTEIN_DISTANCE / HAMMING_DISTANCE --

/// `levenshtein_distance(a, b) -> bigint`: minimum number of
/// single-character insertions, deletions and substitutions to turn
/// `a` into `b` (computed over Unicode code points).
#[derive(Debug)]
struct LevenshteinFunction;

fn levenshtein(a: &str, b: &str) -> i64 {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.is_empty() {
        return b.len() as i64;
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0usize; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        cur[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            cur[j + 1] = (prev[j + 1] + 1).min(cur[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()] as i64
}

impl ScalarFunction for LevenshteinFunction {
    fn name(&self) -> &str {
        "LEVENSHTEIN_DISTANCE"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Int64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("LEVENSHTEIN_DISTANCE", args, 2, 2)?;
        let a = utf8(&args[0], "LEVENSHTEIN_DISTANCE")?;
        let b = utf8(&args[1], "LEVENSHTEIN_DISTANCE")?;
        let out: Int64Array = a
            .iter()
            .zip(b.iter())
            .map(|(x, y)| Some(levenshtein(x?, y?)))
            .collect();
        Ok(Arc::new(out))
    }
}

/// `hamming_distance(a, b) -> bigint`: number of positions at which the
/// characters differ. The strings must have the same length (error
/// otherwise, as in Trino).
#[derive(Debug)]
struct HammingFunction;

impl ScalarFunction for HammingFunction {
    fn name(&self) -> &str {
        "HAMMING_DISTANCE"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Int64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("HAMMING_DISTANCE", args, 2, 2)?;
        let a = utf8(&args[0], "HAMMING_DISTANCE")?;
        let b = utf8(&args[1], "HAMMING_DISTANCE")?;
        let out: Int64Array = a
            .iter()
            .zip(b.iter())
            .map(|(x, y)| match (x, y) {
                (Some(x), Some(y)) => {
                    if x.chars().count() != y.chars().count() {
                        return Err(invalid(
                            "HAMMING_DISTANCE: the input strings must have the same length",
                        ));
                    }
                    Ok(Some(
                        x.chars().zip(y.chars()).filter(|(p, q)| p != q).count() as i64,
                    ))
                }
                _ => Ok(None),
            })
            .collect::<Result<_, _>>()?;
        Ok(Arc::new(out))
    }
}

// -- TRANSLATE --

/// `translate(source, from, to) -> varchar`: replaces each character of
/// `source` found in `from` with the character at the same position in
/// `to`; characters of `from` with no counterpart in `to` are deleted.
/// If a character appears more than once in `from`, its first
/// occurrence wins (Trino semantics).
#[derive(Debug)]
struct TranslateFunction;

impl ScalarFunction for TranslateFunction {
    fn name(&self) -> &str {
        "TRANSLATE"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("TRANSLATE", args, 3, 3)?;
        let s = utf8(&args[0], "TRANSLATE")?;
        let from = utf8(&args[1], "TRANSLATE")?;
        let to = utf8(&args[2], "TRANSLATE")?;
        let mut out = StringBuilder::with_capacity(s.len(), s.value_data().len());
        for i in 0..s.len() {
            if s.is_null(i) || from.is_null(i) || to.is_null(i) {
                out.append_null();
                continue;
            }
            let to_chars: Vec<char> = to.value(i).chars().collect();
            let mut map: std::collections::HashMap<char, Option<char>> =
                std::collections::HashMap::new();
            for (k, c) in from.value(i).chars().enumerate() {
                map.entry(c).or_insert_with(|| to_chars.get(k).copied());
            }
            let translated: String = s
                .value(i)
                .chars()
                .filter_map(|c| match map.get(&c) {
                    Some(mapped) => *mapped,
                    None => Some(c),
                })
                .collect();
            out.append_value(translated);
        }
        Ok(Arc::new(out.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::as_string_array;

    fn make_string_array(vals: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(vals.to_vec()))
    }

    fn make_int64_array(vals: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(vals.to_vec()))
    }

    #[test]
    fn test_upper() {
        let f = UpperFunction;
        let result = f
            .evaluate(&[make_string_array(&[Some("hello"), None, Some("World")])])
            .unwrap();
        let arr = as_string_array(&result);
        assert_eq!(arr.value(0), "HELLO");
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), "WORLD");
    }

    #[test]
    fn test_lower() {
        let f = LowerFunction;
        let result = f
            .evaluate(&[make_string_array(&[Some("HELLO"), Some("")])])
            .unwrap();
        let arr = as_string_array(&result);
        assert_eq!(arr.value(0), "hello");
        assert_eq!(arr.value(1), "");
    }

    #[test]
    fn test_substring() {
        let f = SubstringFunction;
        let result = f
            .evaluate(&[
                make_string_array(&[Some("hello"), Some("world"), None]),
                make_int64_array(&[Some(2), Some(1), Some(1)]),
                make_int64_array(&[Some(3), Some(5), Some(1)]),
            ])
            .unwrap();
        let arr = as_string_array(&result);
        assert_eq!(arr.value(0), "ell"); // SUBSTRING('hello', 2, 3)
        assert_eq!(arr.value(1), "world"); // SUBSTRING('world', 1, 5)
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_trim() {
        let f = TrimFunction;
        let result = f
            .evaluate(&[make_string_array(&[Some("  hello  "), Some(""), None])])
            .unwrap();
        let arr = as_string_array(&result);
        assert_eq!(arr.value(0), "hello");
        assert_eq!(arr.value(1), "");
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_concat() {
        let f = ConcatFunction;
        let result = f
            .evaluate(&[
                make_string_array(&[Some("hello"), Some("a"), None]),
                make_string_array(&[Some(" world"), Some("b"), Some("c")]),
            ])
            .unwrap();
        let arr = as_string_array(&result);
        assert_eq!(arr.value(0), "hello world");
        assert_eq!(arr.value(1), "ab");
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_length() {
        let f = LengthFunction;
        let result = f
            .evaluate(&[make_string_array(&[Some("hello"), Some(""), None])])
            .unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 5);
        assert_eq!(arr.value(1), 0);
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_replace() {
        let f = ReplaceFunction;
        let result = f
            .evaluate(&[
                make_string_array(&[Some("hello world"), Some("aaa")]),
                make_string_array(&[Some("world"), Some("a")]),
                make_string_array(&[Some("rust"), Some("b")]),
            ])
            .unwrap();
        let arr = as_string_array(&result);
        assert_eq!(arr.value(0), "hello rust");
        assert_eq!(arr.value(1), "bbb");
    }

    #[test]
    fn test_position() {
        let f = PositionFunction;
        let result = f
            .evaluate(&[
                make_string_array(&[Some("lo"), Some("xyz"), None]),
                make_string_array(&[Some("hello"), Some("hello"), Some("hello")]),
            ])
            .unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 4); // "lo" in "hello" at position 4
        assert_eq!(arr.value(1), 0); // "xyz" not found
        assert!(arr.is_null(2));
    }

    // -- Trino batch 1 --

    fn strs(v: &[Option<&str>]) -> ArrayRef {
        make_string_array(v)
    }

    fn ints(v: &[Option<i64>]) -> ArrayRef {
        make_int64_array(v)
    }

    fn as_str_vec(a: &ArrayRef) -> Vec<Option<String>> {
        as_string_array(a)
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    fn as_i64_vec(a: &ArrayRef) -> Vec<Option<i64>> {
        a.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    fn s(v: &str) -> Option<String> {
        Some(v.to_string())
    }

    #[test]
    fn upper_of_null_literal_does_not_panic() {
        let out = UpperFunction
            .evaluate(&[Arc::new(arrow::array::NullArray::new(2))])
            .unwrap();
        assert_eq!(out.null_count(), 2);
    }

    #[test]
    fn substring_accepts_int32_positions() {
        let out = SubstringFunction
            .evaluate(&[
                strs(&[Some("hello")]),
                Arc::new(arrow::array::Int32Array::from(vec![2])),
            ])
            .unwrap();
        assert_eq!(as_str_vec(&out), vec![s("ello")]);
    }

    #[test]
    fn trim_with_character_set() {
        let out = TrimFunction
            .evaluate(&[strs(&[Some("xxhixyx"), None]), strs(&[Some("xy"); 2])])
            .unwrap();
        assert_eq!(as_str_vec(&out), vec![s("hi"), None]);
        let out = LtrimFunction
            .evaluate(&[strs(&[Some("xxhix")]), strs(&[Some("x")])])
            .unwrap();
        assert_eq!(as_str_vec(&out), vec![s("hix")]);
        let out = RtrimFunction
            .evaluate(&[strs(&[Some("xxhix")]), strs(&[Some("x")])])
            .unwrap();
        assert_eq!(as_str_vec(&out), vec![s("xxhi")]);
    }

    #[test]
    fn replace_two_argument_form_removes() {
        let out = ReplaceFunction
            .evaluate(&[strs(&[Some("a-b-c")]), strs(&[Some("-")])])
            .unwrap();
        assert_eq!(as_str_vec(&out), vec![s("abc")]);
    }

    #[test]
    fn split_part_semantics() {
        let out = SplitPartFunction
            .evaluate(&[
                strs(&[
                    Some("a,b,c"),
                    Some("a,b,c"),
                    Some("a,,c"),
                    None,
                    Some("abc"),
                ]),
                strs(&[Some(","), Some(","), Some(","), Some(","), Some("")]),
                ints(&[Some(2), Some(4), Some(2), Some(1), Some(3)]),
            ])
            .unwrap();
        assert_eq!(
            as_str_vec(&out),
            vec![s("b"), None, s(""), None, s("c")],
            "index past the end -> NULL; empty field -> ''; empty delimiter splits chars"
        );
        assert!(SplitPartFunction
            .evaluate(&[strs(&[Some("a")]), strs(&[Some(",")]), ints(&[Some(0)])])
            .is_err());
    }

    #[test]
    fn starts_with_semantics() {
        let out = StartsWithFunction
            .evaluate(&[
                strs(&[Some("hello"), Some("hello"), None]),
                strs(&[Some("he"), Some("lo"), Some("x")]),
            ])
            .unwrap();
        let out = out
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(out.value(0));
        assert!(!out.value(1));
        assert!(out.is_null(2));
    }

    #[test]
    fn strpos_semantics() {
        let out = StrposFunction
            .evaluate(&[
                strs(&[Some("high"), Some("high"), Some("héllo"), Some("x"), None]),
                strs(&[Some("ig"), Some("z"), Some("llo"), Some(""), Some("a")]),
            ])
            .unwrap();
        assert_eq!(
            as_i64_vec(&out),
            vec![Some(2), Some(0), Some(3), Some(1), None]
        );
        let out = StrposFunction
            .evaluate(&[
                strs(&[Some("abcabc"), Some("aaa"), Some("abc")]),
                strs(&[Some("bc"), Some("aa"), Some("b")]),
                ints(&[Some(2), Some(2), Some(2)]),
            ])
            .unwrap();
        assert_eq!(as_i64_vec(&out), vec![Some(5), Some(2), Some(0)]);
        assert!(StrposFunction
            .evaluate(&[strs(&[Some("a")]), strs(&[Some("a")]), ints(&[Some(0)])])
            .is_err());
    }

    #[test]
    fn reverse_is_unicode_aware() {
        let out = ReverseFunction
            .evaluate(&[strs(&[Some("abc"), Some("héllo"), None])])
            .unwrap();
        assert_eq!(as_str_vec(&out), vec![s("cba"), s("olléh"), None]);
    }

    #[test]
    fn lpad_rpad_semantics() {
        let lpad = PadFunction { left: true };
        let rpad = PadFunction { left: false };
        let args = [
            strs(&[Some("hi"), Some("hello"), Some("hi"), None]),
            ints(&[Some(5), Some(3), Some(6), Some(3)]),
            strs(&[Some("xy"), Some("x"), Some("ab"), Some("x")]),
        ];
        assert_eq!(
            as_str_vec(&lpad.evaluate(&args).unwrap()),
            vec![s("xyxhi"), s("hel"), s("ababhi"), None]
        );
        assert_eq!(
            as_str_vec(&rpad.evaluate(&args).unwrap()),
            vec![s("hixyx"), s("hel"), s("hiabab"), None]
        );
        assert!(lpad
            .evaluate(&[strs(&[Some("a")]), ints(&[Some(3)]), strs(&[Some("")])])
            .is_err());
        assert!(lpad
            .evaluate(&[strs(&[Some("a")]), ints(&[Some(-1)]), strs(&[Some("x")])])
            .is_err());
    }

    #[test]
    fn chr_and_codepoint() {
        let out = ChrFunction
            .evaluate(&[ints(&[Some(65), Some(233), None])])
            .unwrap();
        assert_eq!(as_str_vec(&out), vec![s("A"), s("é"), None]);
        assert!(ChrFunction.evaluate(&[ints(&[Some(-1)])]).is_err());
        assert!(ChrFunction.evaluate(&[ints(&[Some(0xD800)])]).is_err());

        let out = CodepointFunction
            .evaluate(&[strs(&[Some("A"), Some("é"), None])])
            .unwrap();
        let out = out
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(out.value(0), 65);
        assert_eq!(out.value(1), 233);
        assert!(out.is_null(2));
        assert!(CodepointFunction.evaluate(&[strs(&[Some("ab")])]).is_err());
    }

    #[test]
    fn concat_ws_skips_nulls_but_null_separator_is_null() {
        let out = ConcatWsFunction
            .evaluate(&[
                strs(&[Some(","), Some(","), None]),
                strs(&[Some("a"), None, Some("a")]),
                strs(&[Some("b"), Some("b"), Some("b")]),
            ])
            .unwrap();
        assert_eq!(as_str_vec(&out), vec![s("a,b"), s("b"), None]);
        let out = ConcatWsFunction.evaluate(&[strs(&[Some(",")])]).unwrap();
        assert_eq!(as_str_vec(&out), vec![s("")]);
    }

    #[test]
    fn levenshtein_and_hamming() {
        let out = LevenshteinFunction
            .evaluate(&[
                strs(&[Some("kitten"), Some(""), Some("abc"), None]),
                strs(&[Some("sitting"), Some("abc"), Some("abc"), Some("a")]),
            ])
            .unwrap();
        assert_eq!(as_i64_vec(&out), vec![Some(3), Some(3), Some(0), None]);

        let out = HammingFunction
            .evaluate(&[
                strs(&[Some("abcde"), None]),
                strs(&[Some("abxdz"), Some("a")]),
            ])
            .unwrap();
        assert_eq!(as_i64_vec(&out), vec![Some(2), None]);
        assert!(HammingFunction
            .evaluate(&[strs(&[Some("ab")]), strs(&[Some("abc")])])
            .is_err());
    }

    #[test]
    fn translate_semantics() {
        let out = TranslateFunction
            .evaluate(&[
                strs(&[Some("abcd"), Some("abcd"), Some("hello"), None]),
                strs(&[Some("a"), Some("abc"), Some("ll"), Some("a")]),
                strs(&[Some("z"), Some("x"), Some("LX"), Some("b")]),
            ])
            .unwrap();
        assert_eq!(
            as_str_vec(&out),
            vec![s("zbcd"), s("xd"), s("heLLo"), None],
            "extra 'from' chars are deleted; first mapping of a duplicate wins"
        );
    }
}
