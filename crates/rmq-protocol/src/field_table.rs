use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::BTreeMap;

use crate::error::ProtocolError;

/// AMQP field table: ordered key-value pairs with typed values.
/// Keys are short strings (max 128 bytes per spec), values are typed.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FieldTable(pub BTreeMap<String, FieldValue>);

impl FieldTable {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn insert(&mut self, key: impl Into<String>, value: FieldValue) {
        self.0.insert(key.into(), value);
    }

    pub fn get(&self, key: &str) -> Option<&FieldValue> {
        self.0.get(key)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Calculate the encoded byte size of this field table (excluding the 4-byte length prefix).
    pub fn encoded_size(&self) -> u32 {
        let mut size = 0u32;
        for (key, value) in &self.0 {
            size += 1 + key.len() as u32; // short string: 1 byte len + content
            size += value.encoded_size();
        }
        size
    }

    /// Encode this field table into the buffer (with 4-byte length prefix).
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        let size = self.encoded_size();
        buf.put_u32(size);
        for (key, value) in &self.0 {
            encode_short_string(buf, key)?;
            value.encode(buf)?;
        }
        Ok(())
    }

    /// Decode a field table from the buffer (reads 4-byte length prefix first).
    pub fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        if buf.remaining() < 4 {
            return Err(ProtocolError::Incomplete {
                needed: 4,
                available: buf.remaining(),
            });
        }
        let table_len = buf.get_u32() as usize;
        if buf.remaining() < table_len {
            return Err(ProtocolError::Incomplete {
                needed: table_len,
                available: buf.remaining(),
            });
        }

        let mut table_buf = buf.split_to(table_len);
        let mut table = BTreeMap::new();

        while table_buf.has_remaining() {
            let key = decode_short_string(&mut table_buf)?;
            let value = FieldValue::decode(&mut table_buf)?;
            table.insert(key, value);
        }

        Ok(FieldTable(table))
    }
}

/// AMQP field value types.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Bool(bool),
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    F32(f32),
    F64(f64),
    ShortString(String),
    LongString(Bytes),
    Timestamp(u64),
    FieldTable(FieldTable),
    FieldArray(Vec<FieldValue>),
    Void,
}

impl FieldValue {
    /// Type tag byte for this value.
    fn type_tag(&self) -> u8 {
        match self {
            FieldValue::Bool(_) => b't',
            FieldValue::I8(_) => b'b',
            FieldValue::U8(_) => b'B',
            FieldValue::I16(_) => b'O', // 'O' for "ordinal short" — avoids collision with ShortString 's'
            FieldValue::U16(_) => b'u',
            FieldValue::I32(_) => b'I',
            FieldValue::U32(_) => b'i',
            FieldValue::I64(_) => b'l',
            FieldValue::U64(_) => b'L', // non-standard but used by RabbitMQ
            FieldValue::F32(_) => b'f',
            FieldValue::F64(_) => b'd',
            FieldValue::ShortString(_) => b's', // AMQP extension: short string (1-byte length prefix)
            FieldValue::LongString(_) => b'S',
            FieldValue::Timestamp(_) => b'T',
            FieldValue::FieldTable(_) => b'F',
            FieldValue::FieldArray(_) => b'A',
            FieldValue::Void => b'V',
        }
    }

    /// Encoded size including type tag byte.
    pub fn encoded_size(&self) -> u32 {
        1 + match self {
            FieldValue::Bool(_) => 1,
            FieldValue::I8(_) | FieldValue::U8(_) => 1,
            FieldValue::I16(_) | FieldValue::U16(_) => 2,
            FieldValue::I32(_) | FieldValue::U32(_) => 4,
            FieldValue::I64(_) | FieldValue::U64(_) => 8,
            FieldValue::F32(_) => 4,
            FieldValue::F64(_) => 8,
            FieldValue::ShortString(s) => 1 + s.len() as u32, // 1-byte length prefix
            FieldValue::LongString(b) => 4 + b.len() as u32,
            FieldValue::Timestamp(_) => 8,
            FieldValue::FieldTable(t) => 4 + t.encoded_size(),
            FieldValue::FieldArray(arr) => 4 + arr.iter().map(|v| v.encoded_size()).sum::<u32>(),
            FieldValue::Void => 0,
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        buf.put_u8(self.type_tag());
        match self {
            FieldValue::Bool(v) => buf.put_u8(if *v { 1 } else { 0 }),
            FieldValue::I8(v) => buf.put_i8(*v),
            FieldValue::U8(v) => buf.put_u8(*v),
            FieldValue::I16(v) => buf.put_i16(*v),
            FieldValue::U16(v) => buf.put_u16(*v),
            FieldValue::I32(v) => buf.put_i32(*v),
            FieldValue::U32(v) => buf.put_u32(*v),
            FieldValue::I64(v) => buf.put_i64(*v),
            FieldValue::U64(v) => buf.put_u64(*v),
            FieldValue::F32(v) => buf.put_f32(*v),
            FieldValue::F64(v) => buf.put_f64(*v),
            FieldValue::ShortString(s) => {
                buf.put_u8(s.len() as u8); // 1-byte length prefix for short strings
                buf.put_slice(s.as_bytes());
            }
            FieldValue::LongString(b) => {
                buf.put_u32(b.len() as u32);
                buf.put_slice(b);
            }
            FieldValue::Timestamp(v) => buf.put_u64(*v),
            FieldValue::FieldTable(t) => {
                t.encode(buf)?;
                return Ok(());
            }
            FieldValue::FieldArray(arr) => {
                let size: u32 = arr.iter().map(|v| v.encoded_size()).sum();
                buf.put_u32(size);
                for v in arr {
                    v.encode(buf)?;
                }
            }
            FieldValue::Void => {}
        }
        Ok(())
    }

    pub fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        if !buf.has_remaining() {
            return Err(ProtocolError::Incomplete {
                needed: 1,
                available: 0,
            });
        }
        let tag = buf.get_u8();
        match tag {
            b't' => {
                ensure_remaining(buf, 1)?;
                Ok(FieldValue::Bool(buf.get_u8() != 0))
            }
            b'b' => {
                ensure_remaining(buf, 1)?;
                Ok(FieldValue::I8(buf.get_i8()))
            }
            b'B' => {
                ensure_remaining(buf, 1)?;
                Ok(FieldValue::U8(buf.get_u8()))
            }
            b's' => {
                // AMQP extension: short string with 1-byte length prefix
                ensure_remaining(buf, 1)?;
                let len = buf.get_u8() as usize;
                ensure_remaining(buf, len)?;
                let data = buf.split_to(len);
                let s = std::str::from_utf8(&data).map_err(|e| {
                    ProtocolError::InvalidFieldTable(format!("invalid UTF-8 in short string: {e}"))
                })?;
                Ok(FieldValue::ShortString(s.to_string()))
            }
            b'O' => {
                // I16 (moved from 's' to avoid collision with ShortString)
                ensure_remaining(buf, 2)?;
                Ok(FieldValue::I16(buf.get_i16()))
            }
            b'u' => {
                ensure_remaining(buf, 2)?;
                Ok(FieldValue::U16(buf.get_u16()))
            }
            b'I' => {
                ensure_remaining(buf, 4)?;
                Ok(FieldValue::I32(buf.get_i32()))
            }
            b'i' => {
                ensure_remaining(buf, 4)?;
                Ok(FieldValue::U32(buf.get_u32()))
            }
            b'l' => {
                ensure_remaining(buf, 8)?;
                Ok(FieldValue::I64(buf.get_i64()))
            }
            b'L' => {
                ensure_remaining(buf, 8)?;
                Ok(FieldValue::U64(buf.get_u64()))
            }
            b'f' => {
                ensure_remaining(buf, 4)?;
                Ok(FieldValue::F32(buf.get_f32()))
            }
            b'd' => {
                ensure_remaining(buf, 8)?;
                Ok(FieldValue::F64(buf.get_f64()))
            }
            b'S' => {
                ensure_remaining(buf, 4)?;
                let len = buf.get_u32() as usize;
                ensure_remaining(buf, len)?;
                let data = buf.split_to(len);
                Ok(FieldValue::LongString(data))
            }
            b'T' => {
                ensure_remaining(buf, 8)?;
                Ok(FieldValue::Timestamp(buf.get_u64()))
            }
            b'F' => Ok(FieldValue::FieldTable(FieldTable::decode(buf)?)),
            b'A' => {
                ensure_remaining(buf, 4)?;
                let array_len = buf.get_u32() as usize;
                ensure_remaining(buf, array_len)?;
                let mut array_buf = buf.split_to(array_len);
                let mut arr = Vec::new();
                while array_buf.has_remaining() {
                    arr.push(FieldValue::decode(&mut array_buf)?);
                }
                Ok(FieldValue::FieldArray(arr))
            }
            b'V' => Ok(FieldValue::Void),
            _ => Err(ProtocolError::InvalidFieldTable(format!(
                "unknown field type tag: {tag:#x}"
            ))),
        }
    }
}

fn ensure_remaining(buf: &Bytes, needed: usize) -> Result<(), ProtocolError> {
    if buf.remaining() < needed {
        Err(ProtocolError::Incomplete {
            needed,
            available: buf.remaining(),
        })
    } else {
        Ok(())
    }
}

pub fn encode_short_string(buf: &mut BytesMut, s: &str) -> Result<(), ProtocolError> {
    if s.len() > 255 {
        return Err(ProtocolError::StringTooLong {
            len: s.len(),
            max: 255,
        });
    }
    buf.put_u8(s.len() as u8);
    buf.put_slice(s.as_bytes());
    Ok(())
}

pub fn decode_short_string(buf: &mut Bytes) -> Result<String, ProtocolError> {
    if !buf.has_remaining() {
        return Err(ProtocolError::Incomplete {
            needed: 1,
            available: 0,
        });
    }
    let len = buf.get_u8() as usize;
    if buf.remaining() < len {
        return Err(ProtocolError::Incomplete {
            needed: len,
            available: buf.remaining(),
        });
    }
    let data = buf.split_to(len);
    String::from_utf8(data.to_vec()).map_err(|e| {
        ProtocolError::InvalidFieldTable(format!("invalid UTF-8 in short string: {e}"))
    })
}

pub fn encode_long_string(buf: &mut BytesMut, s: &[u8]) {
    buf.put_u32(s.len() as u32);
    buf.put_slice(s);
}

pub fn decode_long_string(buf: &mut Bytes) -> Result<Bytes, ProtocolError> {
    if buf.remaining() < 4 {
        return Err(ProtocolError::Incomplete {
            needed: 4,
            available: buf.remaining(),
        });
    }
    let len = buf.get_u32() as usize;
    if buf.remaining() < len {
        return Err(ProtocolError::Incomplete {
            needed: len,
            available: buf.remaining(),
        });
    }
    Ok(buf.split_to(len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_table_round_trip() {
        let mut table = FieldTable::new();
        table.insert("bool", FieldValue::Bool(true));
        table.insert("int", FieldValue::I32(42));
        table.insert("string", FieldValue::ShortString("hello".into()));
        table.insert("void", FieldValue::Void);
        table.insert("timestamp", FieldValue::Timestamp(1234567890));

        let mut nested = FieldTable::new();
        nested.insert("inner", FieldValue::I64(-999));
        table.insert("nested", FieldValue::FieldTable(nested));

        table.insert(
            "array",
            FieldValue::FieldArray(vec![
                FieldValue::Bool(false),
                FieldValue::I32(1),
                FieldValue::ShortString("two".into()),
            ]),
        );

        let mut buf = BytesMut::new();
        table.encode(&mut buf).unwrap();

        let mut bytes = buf.freeze();
        let decoded = FieldTable::decode(&mut bytes).unwrap();
        assert_eq!(table, decoded);
    }

    #[test]
    fn test_short_string_round_trip() {
        let mut buf = BytesMut::new();
        encode_short_string(&mut buf, "test").unwrap();
        let mut bytes = buf.freeze();
        let decoded = decode_short_string(&mut bytes).unwrap();
        assert_eq!(decoded, "test");
    }

    #[test]
    fn test_encode_short_string_too_long() {
        let long = "x".repeat(256);
        let mut buf = BytesMut::new();
        let result = encode_short_string(&mut buf, &long);
        assert!(matches!(
            result,
            Err(ProtocolError::StringTooLong { len: 256, max: 255 })
        ));
        // Buffer should be untouched
        assert!(buf.is_empty());
    }

    #[test]
    fn test_encode_short_string_max_length() {
        let max_str = "a".repeat(255);
        let mut buf = BytesMut::new();
        encode_short_string(&mut buf, &max_str).unwrap();
        let mut bytes = buf.freeze();
        let decoded = decode_short_string(&mut bytes).unwrap();
        assert_eq!(decoded, max_str);
    }

    #[test]
    fn test_short_string_and_long_string_distinct_tags() {
        let short = FieldValue::ShortString("hello".into());
        let long = FieldValue::LongString(Bytes::from_static(b"world"));

        // Verify they use different type tags
        assert_eq!(short.type_tag(), b's');
        assert_eq!(long.type_tag(), b'S');
        assert_ne!(short.type_tag(), long.type_tag());
    }

    #[test]
    fn test_short_string_field_value_round_trip() {
        let value = FieldValue::ShortString("test-string".into());
        let mut buf = BytesMut::new();
        value.encode(&mut buf).unwrap();
        let mut bytes = buf.freeze();
        let decoded = FieldValue::decode(&mut bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_long_string_field_value_round_trip() {
        let value = FieldValue::LongString(Bytes::from_static(b"binary\x00data"));
        let mut buf = BytesMut::new();
        value.encode(&mut buf).unwrap();
        let mut bytes = buf.freeze();
        let decoded = FieldValue::decode(&mut bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_i16_field_value_round_trip_new_tag() {
        // I16 was moved from 's' to 'O' to avoid collision with ShortString
        let value = FieldValue::I16(-1000);
        let mut buf = BytesMut::new();
        value.encode(&mut buf).unwrap();
        // Verify it uses 'O' tag
        assert_eq!(buf[0], b'O');
        let mut bytes = buf.freeze();
        let decoded = FieldValue::decode(&mut bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_mixed_short_long_string_table() {
        let mut table = FieldTable::new();
        table.insert("short", FieldValue::ShortString("compact".into()));
        table.insert(
            "long",
            FieldValue::LongString(Bytes::from_static(b"verbose data here")),
        );

        let mut buf = BytesMut::new();
        table.encode(&mut buf).unwrap();
        let mut bytes = buf.freeze();
        let decoded = FieldTable::decode(&mut bytes).unwrap();
        assert_eq!(table, decoded);
    }

    #[test]
    fn test_long_string_round_trip() {
        let data = b"hello world, this is a longer string";
        let mut buf = BytesMut::new();
        encode_long_string(&mut buf, data);
        let mut bytes = buf.freeze();
        let decoded = decode_long_string(&mut bytes).unwrap();
        assert_eq!(&decoded[..], data);
    }

    #[test]
    fn test_empty_field_table() {
        let table = FieldTable::new();
        let mut buf = BytesMut::new();
        table.encode(&mut buf).unwrap();
        let mut bytes = buf.freeze();
        let decoded = FieldTable::decode(&mut bytes).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_field_value_all_numeric_types() {
        let values = vec![
            FieldValue::I8(-1),
            FieldValue::U8(255),
            FieldValue::I16(-1000),
            FieldValue::U16(65000),
            FieldValue::I32(-100_000),
            FieldValue::U32(100_000),
            FieldValue::I64(-1_000_000_000),
            FieldValue::U64(1_000_000_000),
            FieldValue::F32(3.14),
            FieldValue::F64(2.718281828),
        ];

        for value in values {
            let mut buf = BytesMut::new();
            value.encode(&mut buf).unwrap();
            let mut bytes = buf.freeze();
            let decoded = FieldValue::decode(&mut bytes).unwrap();
            assert_eq!(value, decoded);
        }
    }
}
