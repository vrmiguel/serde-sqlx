use json::PgJson;
use map_access::PgRowMapAccess;
use seq_access::{PgArraySeqAccess, PgRowSeqAccess};
use serde::de::{value::Error as DeError, Deserialize, Deserializer, Visitor};
use serde::de::{Error as _, IntoDeserializer};
use serde::forward_to_deserialize_any;
use sqlx::postgres::{PgRow, PgValueRef};
use sqlx::{Row, TypeInfo, ValueRef};

mod json;
mod map_access;
mod seq_access;

/// Convenience function: deserialize a PgRow into any T that implements Deserialize
pub fn from_pg_row<T>(row: PgRow) -> Result<T, DeError>
where
    T: for<'de> Deserialize<'de>,
{
    let deserializer = PgRowDeserializer::new(&row);
    T::deserialize(deserializer)
}

#[derive(Clone, Copy)]
pub struct PgRowDeserializer<'a> {
    row: &'a PgRow,
    index: usize,
}

impl<'a> PgRowDeserializer<'a> {
    pub fn new(row: &'a PgRow) -> Self {
        PgRowDeserializer { row, index: 0 }
    }
}

impl<'de, 'a> Deserializer<'de> for PgRowDeserializer<'a> {
    type Error = DeError;

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let raw_value = self.row.try_get_raw(0).map_err(DeError::custom)?;

        if raw_value.is_null() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.row.columns().len() {
            0 => return visitor.visit_unit(),
            1 => {}
            n => {
                println!("Columns was {n}");
                return self.deserialize_seq(visitor);
            }
        };

        let raw_value = self.row.try_get_raw(self.index).map_err(DeError::custom)?;
        let type_info = raw_value.type_info();
        let type_name = type_info.name();

        if raw_value.is_null() {
            return visitor.visit_none();
        }

        // If this is a BOOL[], TEXT[], etc
        if type_name.ends_with("[]") {
            return self.deserialize_seq(visitor);
        }

        // Direct all "basic" types down to `PgValueDeserializer`
        let deserializer = PgValueDeserializer { value: raw_value };

        deserializer.deserialize_any(visitor)
    }

    /// We treat the row as a map (each column is a key/value pair)
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(PgRowMapAccess {
            deserializer: self,
            num_cols: self.row.columns().len(),
        })
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("Columns: {}", self.row.columns().len());

        let raw_value = self.row.try_get_raw(self.index).map_err(DeError::custom)?;
        let type_info = raw_value.type_info();
        let type_name = type_info.name();
        println!("Type: {type_name}");

        match type_name {
            "TEXT[]" | "VARCHAR[]" => {
                let seq_access = PgArraySeqAccess::<String>::new(raw_value)?;
                visitor.visit_seq(seq_access)
            }
            "INT4[]" => {
                println!("INT4 found!");
                let seq_access = PgArraySeqAccess::<i32>::new(raw_value)?;
                visitor.visit_seq(seq_access)
            }
            "JSON[]" | "JSONB[]" => {
                let seq_access = PgArraySeqAccess::<PgJson>::new(raw_value)?;
                visitor.visit_seq(seq_access)
            }
            "BOOL[]" => {
                let seq_access = PgArraySeqAccess::<bool>::new(raw_value)?;
                visitor.visit_seq(seq_access)
            }
            _ => {
                let seq_access = PgRowSeqAccess {
                    deserializer: self,
                    num_cols: self.row.columns().len(),
                };

                visitor.visit_seq(seq_access)
            }
        }
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    // For other types, forward to deserialize_any.
    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
        bytes byte_buf unit unit_struct
        tuple_struct enum identifier ignored_any
    }
}

/// An "inner" deserializer
#[derive(Clone)]
struct PgValueDeserializer<'a> {
    value: PgValueRef<'a>,
}

impl<'de, 'a> Deserializer<'de> for PgValueDeserializer<'a> {
    type Error = DeError;

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.value.is_null() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.value.is_null() {
            return visitor.visit_none();
        }
        let type_info = self.value.type_info();

        let type_name = type_info.name();

        match type_name {
            "FLOAT4" => {
                let v = decode_raw_pg::<f32>(self.value)
                    .ok_or_else(|| DeError::custom("Failed to decode FLOAT4"))?;
                visitor.visit_f32(v)
            }
            "FLOAT8" | "NUMERIC" => {
                let v = decode_raw_pg::<f64>(self.value)
                    .ok_or_else(|| DeError::custom("Failed to decode FLOAT8/NUMERIC"))?;
                visitor.visit_f64(v)
            }
            "INT8" => {
                let v = decode_raw_pg::<i64>(self.value)
                    .ok_or_else(|| DeError::custom("Failed to decode INT8"))?;
                visitor.visit_i64(v)
            }
            "INT4" => {
                let v = decode_raw_pg::<i32>(self.value)
                    .ok_or_else(|| DeError::custom("Failed to decode INT4"))?;
                visitor.visit_i32(v)
            }
            "INT2" => {
                let v = decode_raw_pg::<i16>(self.value)
                    .ok_or_else(|| DeError::custom("Failed to decode INT2"))?;
                visitor.visit_i16(v)
            }
            "BOOL" => {
                let v = decode_raw_pg::<bool>(self.value)
                    .ok_or_else(|| DeError::custom("Failed to decode BOOL"))?;
                visitor.visit_bool(v)
            }
            "DATE" => {
                let date = decode_raw_pg::<chrono::NaiveDate>(self.value)
                    .ok_or_else(|| DeError::custom("Failed to decode DATE"))?;
                visitor.visit_string(date.to_string())
            }
            "TIME" | "TIMETZ" => {
                let time = decode_raw_pg::<chrono::NaiveTime>(self.value)
                    .ok_or_else(|| DeError::custom("Failed to decode TIME/TIMETZ"))?;
                visitor.visit_string(time.to_string())
            }
            "TIMESTAMP" | "TIMESTAMPTZ" => {
                let ts = decode_raw_pg::<chrono::DateTime<chrono::FixedOffset>>(self.value)
                    .ok_or_else(|| DeError::custom("Failed to decode TIMESTAMP/TIMESTAMPTZ"))?;
                visitor.visit_string(ts.to_rfc3339())
            }
            "UUID" => {
                let uuid = decode_raw_pg::<uuid::Uuid>(self.value)
                    .ok_or_else(|| DeError::custom("Failed to decode UUID"))?;
                visitor.visit_string(uuid.to_string())
            }
            "BYTEA" => {
                let bytes = decode_raw_pg::<&[u8]>(self.value)
                    .ok_or_else(|| DeError::custom("Failed to decode BYTEA"))?;
                visitor.visit_bytes(bytes)
            }
            "INTERVAL" => {
                let pg_interval = decode_raw_pg::<sqlx::postgres::types::PgInterval>(self.value)
                    .ok_or_else(|| DeError::custom("Failed to decode INTERVAL"))?;
                let secs = pg_interval.microseconds / 1_000_000;
                let nanos = (pg_interval.microseconds % 1_000_000) * 1000;
                let days_duration = chrono::Duration::days(pg_interval.days as i64);
                let duration = chrono::Duration::seconds(secs)
                    + chrono::Duration::nanoseconds(nanos)
                    + days_duration;
                visitor.visit_string(duration.to_string())
            }
            "CHAR" | "TEXT" => {
                let s = decode_raw_pg::<String>(self.value)
                    .ok_or_else(|| DeError::custom("Failed to decode TEXT/CHAR"))?;
                visitor.visit_string(s)
            }
            "JSON" | "JSONB" => {
                let value = decode_raw_pg::<PgJson>(self.value)
                    .ok_or_else(|| DeError::custom("Failed to decode JSON/JSONB"))?;

                value.into_deserializer().deserialize_any(visitor)
            }
            last => {
                println!("In fallback (PgValueDeserializer): last is {last}");
                let as_string = decode_raw_pg::<String>(self.value.clone())
                    .ok_or_else(|| DeError::custom(format!("Failed to decode type {last}")))?;
                visitor.visit_string(as_string)
            }
        }
    }

    // For other types, forward to deserialize_any.
    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
        bytes byte_buf unit unit_struct newtype_struct struct
        tuple_struct enum identifier ignored_any tuple seq map
    }
}

/// Decode a raw Postgres value into a type T using sqlx’s Decode,
/// returning an Option<T> instead of a default value on error.
fn decode_raw_pg<'a, T>(raw_value: PgValueRef<'a>) -> Option<T>
where
    T: sqlx::Decode<'a, sqlx::Postgres>,
{
    match T::decode(raw_value) {
        Ok(v) => Some(v),
        Err(e) => {
            eprintln!(
                "Failed to decode {} value: {:?}",
                std::any::type_name::<T>(),
                e
            );
            None
        }
    }
}
