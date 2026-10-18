//! Decoder for `COPY … TO STDOUT (FORMAT binary)` into Arrow arrays.
//!
//! Wire format (PostgreSQL docs, "COPY → Binary Format"): an 11-byte
//! signature, an i32 flags word and an i32-length header extension; then
//! one tuple per row — an i16 field count followed by, per field, an i32
//! byte length (−1 = NULL) and the value in the type's binary send format;
//! then an i16 −1 trailer. Chunks from the server need not align with
//! tuples, so undecoded bytes are buffered until a whole tuple is present.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, RecordBatch, RecordBatchOptions,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::SchemaRef;
use bytes::{Buf, BytesMut};

use arneb_common::types::DataType;

use crate::time::{PG_EPOCH_DAYS, PG_EPOCH_MICROS};
use crate::types::unsupported_type;

const SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";

/// How one column's binary values are decoded.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Kind {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric { precision: u8, scale: i8 },
    Text,
    Date,
    Timestamp { timezone: Option<Arc<str>> },
    Uuid,
    Bytea,
}

impl Kind {
    /// The decoder for a column of `remote_type` read as `data_type`, or an
    /// error naming the column when its type cannot be read.
    pub(crate) fn for_column(
        name: &str,
        remote_type: &str,
        data_type: &DataType,
    ) -> Result<Self, String> {
        if let Some(desc) = unsupported_type(remote_type) {
            return Err(format!(
                "column '{name}' has unsupported PostgreSQL type '{desc}'"
            ));
        }
        let kind = match (remote_type, data_type) {
            ("bool", DataType::Boolean) => Kind::Bool,
            ("int2", DataType::Int16) => Kind::Int2,
            ("int4", DataType::Int32) => Kind::Int4,
            ("int8", DataType::Int64) => Kind::Int8,
            ("float4", DataType::Float32) => Kind::Float4,
            ("float8", DataType::Float64) => Kind::Float8,
            ("numeric", DataType::Decimal128 { precision, scale }) => Kind::Numeric {
                precision: *precision,
                scale: *scale,
            },
            ("text" | "varchar" | "bpchar" | "name", DataType::Utf8) => Kind::Text,
            ("uuid", DataType::Utf8) => Kind::Uuid,
            ("date", DataType::Date32) => Kind::Date,
            ("timestamp" | "timestamptz", DataType::Timestamp { timezone, .. }) => {
                Kind::Timestamp {
                    timezone: timezone.as_deref().map(Arc::from),
                }
            }
            ("bytea", DataType::Binary) => Kind::Bytea,
            (remote, arneb) => {
                return Err(format!(
                    "column '{name}': PostgreSQL type '{remote}' cannot be read as {arneb}"
                ))
            }
        };
        Ok(kind)
    }
}

enum Builder {
    Bool(BooleanBuilder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Decimal(Decimal128Builder, u8, i8),
    Text(StringBuilder),
    Date(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder, Option<Arc<str>>),
    Binary(BinaryBuilder),
}

impl Builder {
    fn new(kind: &Kind, capacity: usize) -> Self {
        match kind {
            Kind::Bool => Builder::Bool(BooleanBuilder::with_capacity(capacity)),
            Kind::Int2 => Builder::I16(Int16Builder::with_capacity(capacity)),
            Kind::Int4 => Builder::I32(Int32Builder::with_capacity(capacity)),
            Kind::Int8 => Builder::I64(Int64Builder::with_capacity(capacity)),
            Kind::Float4 => Builder::F32(Float32Builder::with_capacity(capacity)),
            Kind::Float8 => Builder::F64(Float64Builder::with_capacity(capacity)),
            Kind::Numeric { precision, scale } => Builder::Decimal(
                Decimal128Builder::with_capacity(capacity),
                *precision,
                *scale,
            ),
            Kind::Text | Kind::Uuid => Builder::Text(StringBuilder::with_capacity(capacity, 0)),
            Kind::Date => Builder::Date(Date32Builder::with_capacity(capacity)),
            Kind::Timestamp { timezone } => Builder::Timestamp(
                TimestampMicrosecondBuilder::with_capacity(capacity),
                timezone.clone(),
            ),
            Kind::Bytea => Builder::Binary(BinaryBuilder::with_capacity(capacity, 0)),
        }
    }

    fn finish(&mut self) -> Result<ArrayRef, String> {
        Ok(match self {
            Builder::Bool(b) => Arc::new(b.finish()),
            Builder::I16(b) => Arc::new(b.finish()),
            Builder::I32(b) => Arc::new(b.finish()),
            Builder::I64(b) => Arc::new(b.finish()),
            Builder::F32(b) => Arc::new(b.finish()),
            Builder::F64(b) => Arc::new(b.finish()),
            Builder::Decimal(b, p, s) => Arc::new(
                b.finish()
                    .with_precision_and_scale(*p, *s)
                    .map_err(|e| e.to_string())?,
            ),
            Builder::Text(b) => Arc::new(b.finish()),
            Builder::Date(b) => Arc::new(b.finish()),
            Builder::Timestamp(b, tz) => Arc::new(b.finish().with_timezone_opt(tz.clone())),
            Builder::Binary(b) => Arc::new(b.finish()),
        })
    }

    fn append_null(&mut self) {
        match self {
            Builder::Bool(b) => b.append_null(),
            Builder::I16(b) => b.append_null(),
            Builder::I32(b) => b.append_null(),
            Builder::I64(b) => b.append_null(),
            Builder::F32(b) => b.append_null(),
            Builder::F64(b) => b.append_null(),
            Builder::Decimal(b, ..) => b.append_null(),
            Builder::Text(b) => b.append_null(),
            Builder::Date(b) => b.append_null(),
            Builder::Timestamp(b, _) => b.append_null(),
            Builder::Binary(b) => b.append_null(),
        }
    }

    /// Append one non-NULL value; `kind` disambiguates text vs uuid.
    fn append(&mut self, kind: &Kind, v: &[u8]) -> Result<(), String> {
        let fixed = |n: usize| -> Result<(), String> {
            if v.len() == n {
                Ok(())
            } else {
                Err(format!("expected {n} bytes, got {}", v.len()))
            }
        };
        match self {
            Builder::Bool(b) => {
                fixed(1)?;
                b.append_value(v[0] != 0);
            }
            Builder::I16(b) => {
                fixed(2)?;
                b.append_value(i16::from_be_bytes([v[0], v[1]]));
            }
            Builder::I32(b) => {
                fixed(4)?;
                b.append_value(be_i32(v));
            }
            Builder::I64(b) => {
                fixed(8)?;
                b.append_value(be_i64(v));
            }
            Builder::F32(b) => {
                fixed(4)?;
                b.append_value(f32::from_bits(be_i32(v) as u32));
            }
            Builder::F64(b) => {
                fixed(8)?;
                b.append_value(f64::from_bits(be_i64(v) as u64));
            }
            Builder::Decimal(b, precision, scale) => {
                b.append_value(decode_numeric(v, *precision, *scale)?);
            }
            Builder::Text(b) if *kind == Kind::Uuid => {
                fixed(16)?;
                b.append_value(format_uuid(v));
            }
            Builder::Text(b) => {
                let s = std::str::from_utf8(v).map_err(|e| format!("invalid UTF-8: {e}"))?;
                b.append_value(s);
            }
            Builder::Date(b) => {
                fixed(4)?;
                let days = be_i32(v);
                if days == i32::MAX || days == i32::MIN {
                    return Err("date 'infinity' / '-infinity' is not supported".into());
                }
                let days = days.checked_add(PG_EPOCH_DAYS).ok_or("date out of range")?;
                b.append_value(days);
            }
            Builder::Timestamp(b, _) => {
                fixed(8)?;
                let micros = be_i64(v);
                if micros == i64::MAX || micros == i64::MIN {
                    return Err("timestamp 'infinity' / '-infinity' is not supported".into());
                }
                let micros = micros
                    .checked_add(PG_EPOCH_MICROS)
                    .ok_or("timestamp out of range")?;
                b.append_value(micros);
            }
            Builder::Binary(b) => b.append_value(v),
        }
        Ok(())
    }
}

fn be_i32(v: &[u8]) -> i32 {
    i32::from_be_bytes([v[0], v[1], v[2], v[3]])
}

fn be_i64(v: &[u8]) -> i64 {
    i64::from_be_bytes([v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]])
}

fn format_uuid(v: &[u8]) -> String {
    let hex: String = v.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// The unscaled `Decimal128` value at `scale` of a binary `numeric`, or an
/// error if the value cannot be represented exactly in
/// `DECIMAL(precision, scale)`.
pub(crate) fn decode_numeric(v: &[u8], precision: u8, scale: i8) -> Result<i128, String> {
    if v.len() < 8 {
        return Err("truncated numeric".into());
    }
    let ndigits = u16::from_be_bytes([v[0], v[1]]) as usize;
    let weight = i16::from_be_bytes([v[2], v[3]]) as i32;
    let sign = u16::from_be_bytes([v[4], v[5]]);
    match sign {
        0x0000 | 0x4000 => {}
        0xC000 => return Err("numeric NaN cannot be represented as DECIMAL".into()),
        0xD000 | 0xF000 => return Err("numeric infinity cannot be represented as DECIMAL".into()),
        other => return Err(format!("invalid numeric sign 0x{other:04x}")),
    }
    if v.len() != 8 + 2 * ndigits {
        return Err("truncated numeric".into());
    }
    let overflow = || format!("numeric value does not fit DECIMAL({precision},{scale})");
    let mut acc: i128 = 0;
    for i in 0..ndigits {
        let digit = u16::from_be_bytes([v[8 + 2 * i], v[9 + 2 * i]]) as i128;
        if digit >= 10_000 {
            return Err("invalid numeric digit".into());
        }
        // This base-10000 digit is worth digit × 10^(4·(weight−i)); at the
        // target scale, × 10^exp with exp = 4·(weight−i) + scale.
        let exp = 4 * (weight - i as i32) + scale as i32;
        let term = if exp >= 0 {
            let factor = 10i128.checked_pow(exp as u32).ok_or_else(overflow)?;
            digit.checked_mul(factor).ok_or_else(overflow)?
        } else {
            let divisor = 10i128.pow((-exp).min(4) as u32);
            if (-exp) >= 4 || digit % divisor != 0 {
                if digit != 0 {
                    return Err(format!(
                        "numeric value has more than {scale} fractional digits; \
                         it cannot be read as DECIMAL({precision},{scale}) without rounding"
                    ));
                }
                0
            } else {
                digit / divisor
            }
        };
        acc = acc.checked_add(term).ok_or_else(overflow)?;
    }
    if acc >= 10i128.pow(precision as u32) {
        return Err(overflow());
    }
    Ok(if sign == 0x4000 { -acc } else { acc })
}

/// Incremental binary-COPY decoder producing record batches.
pub(crate) struct CopyDecoder {
    schema: SchemaRef,
    kinds: Vec<Kind>,
    names: Vec<String>,
    builders: Vec<Builder>,
    capacity: usize,
    buf: BytesMut,
    header_done: bool,
    finished: bool,
    rows: usize,
}

impl CopyDecoder {
    /// A decoder whose batches have `schema`; `columns` gives each output
    /// column's name and decoder, in order.
    pub(crate) fn new(schema: SchemaRef, columns: Vec<(String, Kind)>, capacity: usize) -> Self {
        let (names, kinds): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
        let builders = kinds.iter().map(|k| Builder::new(k, capacity)).collect();
        Self {
            schema,
            kinds,
            names,
            builders,
            capacity,
            buf: BytesMut::new(),
            header_done: false,
            finished: false,
            rows: 0,
        }
    }

    /// Buffer the next chunk of COPY data.
    pub(crate) fn extend(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    /// Rows decoded into the current batch.
    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    /// Whether the trailer has been read.
    pub(crate) fn is_finished(&self) -> bool {
        self.finished
    }

    /// Decode buffered tuples until the batch holds `max_rows`, the buffer
    /// has no complete tuple, or the trailer is reached.
    pub(crate) fn decode(&mut self, max_rows: usize) -> Result<(), String> {
        if !self.header_done && !self.read_header()? {
            return Ok(());
        }
        while !self.finished && self.rows < max_rows {
            let Some(next) = self.next_tuple()? else {
                break;
            };
            let Tuple::Row(len) = next else {
                // The trailer (field count −1).
                self.buf.advance(2);
                self.finished = true;
                if !self.buf.is_empty() {
                    return Err("unexpected data after the COPY trailer".into());
                }
                break;
            };
            let tuple = self.buf.split_to(len);
            let mut pos = 2;
            for i in 0..self.kinds.len() {
                let flen = be_i32(&tuple[pos..pos + 4]);
                pos += 4;
                if flen < 0 {
                    self.builders[i].append_null();
                } else {
                    let end = pos + flen as usize;
                    self.builders[i]
                        .append(&self.kinds[i], &tuple[pos..end])
                        .map_err(|e| format!("column '{}': {e}", self.names[i]))?;
                    pos = end;
                }
            }
            self.rows += 1;
        }
        Ok(())
    }

    /// Error unless the stream ended cleanly at the trailer.
    pub(crate) fn check_complete(&self) -> Result<(), String> {
        if self.finished {
            Ok(())
        } else {
            Err("COPY stream ended before its trailer".into())
        }
    }

    /// Take the decoded rows as a batch and start a new one.
    pub(crate) fn take_batch(&mut self) -> Result<RecordBatch, String> {
        let columns = self
            .builders
            .iter_mut()
            .map(Builder::finish)
            .collect::<Result<Vec<_>, _>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(self.rows));
        let batch = RecordBatch::try_new_with_options(self.schema.clone(), columns, &options)
            .map_err(|e| e.to_string())?;
        self.rows = 0;
        self.builders = self
            .kinds
            .iter()
            .map(|k| Builder::new(k, self.capacity))
            .collect();
        Ok(batch)
    }

    /// `Ok(true)` once the header is consumed; `Ok(false)` if more bytes
    /// are needed.
    fn read_header(&mut self) -> Result<bool, String> {
        if self.buf.len() < SIGNATURE.len() + 8 {
            return Ok(false);
        }
        if &self.buf[..SIGNATURE.len()] != SIGNATURE {
            return Err("not a binary COPY stream (bad signature)".into());
        }
        let ext = be_i32(&self.buf[SIGNATURE.len() + 4..]);
        if ext < 0 {
            return Err("invalid COPY header extension length".into());
        }
        let total = SIGNATURE.len() + 8 + ext as usize;
        if self.buf.len() < total {
            return Ok(false);
        }
        self.buf.advance(total);
        self.header_done = true;
        Ok(true)
    }

    /// The next tuple if it is fully buffered, `None` if more bytes are
    /// needed.
    fn next_tuple(&self) -> Result<Option<Tuple>, String> {
        let b = &self.buf[..];
        if b.len() < 2 {
            return Ok(None);
        }
        let count = i16::from_be_bytes([b[0], b[1]]);
        if count == -1 {
            return Ok(Some(Tuple::Trailer));
        }
        if count as usize != self.kinds.len() || count < 0 {
            return Err(format!(
                "COPY tuple has {count} fields, expected {}",
                self.kinds.len()
            ));
        }
        let mut pos = 2;
        for _ in 0..count {
            if b.len() < pos + 4 {
                return Ok(None);
            }
            let flen = be_i32(&b[pos..]);
            pos += 4;
            if flen >= 0 {
                pos += flen as usize;
            } else if flen != -1 {
                return Err(format!("invalid COPY field length {flen}"));
            }
        }
        Ok((b.len() >= pos).then_some(Tuple::Row(pos)))
    }
}

/// What the next buffered tuple is.
enum Tuple {
    /// A row of this many bytes (including its field count).
    Row(usize),
    /// The end-of-data marker.
    Trailer,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray, Decimal128Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{
        DataType as ArrowType, Date32Type, Field, Int32Type, Int64Type, Schema, TimeUnit,
    };

    /// Encode a binary COPY stream of `rows` (each field `None` = NULL).
    fn encode(rows: &[Vec<Option<Vec<u8>>>]) -> Vec<u8> {
        let mut out = SIGNATURE.to_vec();
        out.extend(0i32.to_be_bytes());
        out.extend(4i32.to_be_bytes());
        out.extend([9, 9, 9, 9]); // header extension, skipped
        for row in rows {
            out.extend((row.len() as i16).to_be_bytes());
            for field in row {
                match field {
                    None => out.extend((-1i32).to_be_bytes()),
                    Some(v) => {
                        out.extend((v.len() as i32).to_be_bytes());
                        out.extend(v);
                    }
                }
            }
        }
        out.extend((-1i16).to_be_bytes());
        out
    }

    /// Binary `numeric`: (ndigits, weight, sign, dscale, digits).
    fn numeric(weight: i16, negative: bool, dscale: u16, digits: &[u16]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend((digits.len() as u16).to_be_bytes());
        v.extend(weight.to_be_bytes());
        v.extend((if negative { 0x4000u16 } else { 0 }).to_be_bytes());
        v.extend(dscale.to_be_bytes());
        for d in digits {
            v.extend(d.to_be_bytes());
        }
        v
    }

    fn decoder(fields: Vec<(&str, ArrowType, Kind)>, capacity: usize) -> CopyDecoder {
        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|(n, t, _)| Field::new(*n, t.clone(), true))
                .collect::<Vec<_>>(),
        ));
        let cols = fields
            .into_iter()
            .map(|(n, _, k)| (n.to_string(), k))
            .collect();
        CopyDecoder::new(schema, cols, capacity)
    }

    fn run(dec: &mut CopyDecoder, bytes: &[u8], batch: usize) -> Vec<RecordBatch> {
        dec.extend(bytes);
        let mut out = Vec::new();
        loop {
            dec.decode(batch).unwrap();
            if dec.rows() >= batch || (dec.is_finished() && dec.rows() > 0) {
                out.push(dec.take_batch().unwrap());
            } else if dec.is_finished() || dec.rows() == 0 {
                break;
            }
        }
        out
    }

    #[test]
    fn numeric_decoding_is_exact() {
        // 123.45 = digits [123, 4500], weight 0.
        assert_eq!(
            decode_numeric(&numeric(0, false, 2, &[123, 4500]), 15, 2),
            Ok(12345)
        );
        // -0.05 = digits [500], weight -1.
        assert_eq!(decode_numeric(&numeric(-1, true, 2, &[500]), 15, 2), Ok(-5));
        // 1,000,000 = digits [100], weight 1 → at scale 2: 100000000.
        assert_eq!(
            decode_numeric(&numeric(1, false, 0, &[100]), 15, 2),
            Ok(100_000_000)
        );
        // Zero has no digits.
        assert_eq!(decode_numeric(&numeric(0, false, 0, &[]), 15, 2), Ok(0));
        // 1.000000000001 at scale 10 would round.
        let v = numeric(0, false, 12, &[1, 0, 0, 1]);
        assert!(decode_numeric(&v, 38, 10)
            .unwrap_err()
            .contains("fractional"));
        // 1.5 at scale 0 would round.
        assert!(decode_numeric(&numeric(0, false, 1, &[1, 5000]), 10, 0).is_err());
        // 12345.67 does not fit DECIMAL(5,2).
        assert!(
            decode_numeric(&numeric(1, false, 2, &[1, 2345, 6700]), 5, 2)
                .unwrap_err()
                .contains("does not fit")
        );
    }

    #[test]
    fn numeric_special_values_fail() {
        let mut nan = numeric(0, false, 0, &[]);
        nan[4..6].copy_from_slice(&0xC000u16.to_be_bytes());
        assert!(decode_numeric(&nan, 38, 2).unwrap_err().contains("NaN"));
        let mut inf = numeric(0, false, 0, &[]);
        inf[4..6].copy_from_slice(&0xD000u16.to_be_bytes());
        assert!(decode_numeric(&inf, 38, 2)
            .unwrap_err()
            .contains("infinity"));
    }

    #[test]
    fn decodes_every_type_with_nulls() {
        let uuid: Vec<u8> = (0u8..16).collect();
        let rows = vec![
            vec![
                Some(vec![1]),
                Some(7i16.to_be_bytes().to_vec()),
                Some((-3i32).to_be_bytes().to_vec()),
                Some(9_000_000_000i64.to_be_bytes().to_vec()),
                Some(1.5f32.to_bits().to_be_bytes().to_vec()),
                Some((-2.25f64).to_bits().to_be_bytes().to_vec()),
                Some(numeric(0, false, 2, &[123, 4500])),
                Some(b"R ".to_vec()),
                Some(0i32.to_be_bytes().to_vec()),
                Some(1_000_000i64.to_be_bytes().to_vec()),
                Some(uuid.clone()),
                Some(vec![0xde, 0xad]),
            ],
            vec![None; 12],
        ];
        let utc: Option<Arc<str>> = Some("UTC".into());
        let mut dec = decoder(
            vec![
                ("b", ArrowType::Boolean, Kind::Bool),
                ("i2", ArrowType::Int16, Kind::Int2),
                ("i4", ArrowType::Int32, Kind::Int4),
                ("i8", ArrowType::Int64, Kind::Int8),
                ("f4", ArrowType::Float32, Kind::Float4),
                ("f8", ArrowType::Float64, Kind::Float8),
                (
                    "n",
                    ArrowType::Decimal128(15, 2),
                    Kind::Numeric {
                        precision: 15,
                        scale: 2,
                    },
                ),
                ("t", ArrowType::Utf8, Kind::Text),
                ("d", ArrowType::Date32, Kind::Date),
                (
                    "ts",
                    ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    Kind::Timestamp { timezone: utc },
                ),
                ("u", ArrowType::Utf8, Kind::Uuid),
                ("by", ArrowType::Binary, Kind::Bytea),
            ],
            8,
        );
        let batches = run(&mut dec, &encode(&rows), 8192);
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        assert_eq!(b.num_rows(), 2);
        assert!(b.column(0).as_boolean().value(0));
        assert_eq!(b.column(2).as_primitive::<Int32Type>().value(0), -3);
        assert_eq!(
            b.column(3).as_primitive::<Int64Type>().value(0),
            9_000_000_000
        );
        let n = b
            .column(6)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(n.value(0), 12345);
        let t = b.column(7).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(t.value(0), "R ");
        // PostgreSQL day 0 is 2000-01-01.
        assert_eq!(
            b.column(8).as_primitive::<Date32Type>().value(0),
            PG_EPOCH_DAYS
        );
        let ts = b
            .column(9)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), PG_EPOCH_MICROS + 1_000_000);
        let u = b.column(10).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(u.value(0), "00010203-0405-0607-0809-0a0b0c0d0e0f");
        for c in b.columns() {
            assert!(c.is_null(1));
        }
    }

    #[test]
    fn batches_respect_the_batch_size_across_chunk_boundaries() {
        let rows: Vec<_> = (0..20i64)
            .map(|i| vec![Some(i.to_be_bytes().to_vec())])
            .collect();
        let bytes = encode(&rows);
        let mut dec = decoder(vec![("k", ArrowType::Int64, Kind::Int8)], 4);
        // Feed one byte at a time.
        let mut sizes = Vec::new();
        let mut seen = Vec::new();
        for byte in &bytes {
            dec.extend(std::slice::from_ref(byte));
            loop {
                dec.decode(8).unwrap();
                if dec.rows() >= 8 {
                    let b = dec.take_batch().unwrap();
                    sizes.push(b.num_rows());
                    seen.extend(
                        b.column(0)
                            .as_primitive::<Int64Type>()
                            .values()
                            .iter()
                            .copied(),
                    );
                } else {
                    break;
                }
            }
        }
        assert!(dec.is_finished());
        let b = dec.take_batch().unwrap();
        sizes.push(b.num_rows());
        seen.extend(
            b.column(0)
                .as_primitive::<Int64Type>()
                .values()
                .iter()
                .copied(),
        );
        assert_eq!(sizes, vec![8, 8, 4]);
        assert_eq!(seen, (0..20).collect::<Vec<_>>());
        dec.check_complete().unwrap();
    }

    #[test]
    fn zero_column_rows_are_counted() {
        let rows = vec![vec![]; 3];
        let mut dec = decoder(vec![], 4);
        let batches = run(&mut dec, &encode(&rows), 8192);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 0);
    }

    #[test]
    fn errors_name_the_column() {
        let rows = vec![vec![Some(vec![1, 2, 3])]];
        let mut dec = decoder(vec![("k", ArrowType::Int32, Kind::Int4)], 4);
        dec.extend(&encode(&rows));
        let err = dec.decode(10).unwrap_err();
        assert!(err.contains("column 'k'"), "{err}");
    }

    #[test]
    fn infinite_dates_fail() {
        let rows = vec![vec![Some(i32::MAX.to_be_bytes().to_vec())]];
        let mut dec = decoder(vec![("d", ArrowType::Date32, Kind::Date)], 4);
        dec.extend(&encode(&rows));
        assert!(dec.decode(10).unwrap_err().contains("infinity"));
    }

    #[test]
    fn truncated_stream_is_detected() {
        let rows = vec![vec![Some(1i32.to_be_bytes().to_vec())]];
        let bytes = encode(&rows);
        let mut dec = decoder(vec![("k", ArrowType::Int32, Kind::Int4)], 4);
        dec.extend(&bytes[..bytes.len() - 2]);
        dec.decode(10).unwrap();
        assert!(dec.check_complete().is_err());
    }

    #[test]
    fn wrong_field_count_and_signature_fail() {
        let rows = vec![vec![None, None]];
        let mut dec = decoder(vec![("k", ArrowType::Int32, Kind::Int4)], 4);
        dec.extend(&encode(&rows));
        assert!(dec.decode(10).is_err());

        let mut bad = decoder(vec![], 4);
        bad.extend(b"NOTCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0");
        assert!(bad.decode(10).is_err());
    }

    #[test]
    fn kind_for_column_reports_unsupported_types() {
        let err = Kind::for_column("doc", "!jsonb", &DataType::Utf8).unwrap_err();
        assert_eq!(err, "column 'doc' has unsupported PostgreSQL type 'jsonb'");
        assert_eq!(
            Kind::for_column("id", "int8", &DataType::Int64).unwrap(),
            Kind::Int8
        );
        assert!(Kind::for_column("id", "int8", &DataType::Int32).is_err());
    }
}
