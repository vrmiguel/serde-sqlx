use map_access::{JsonValueMapAccess, PgRowMapAccess};
use seq_access::{PgArraySeqAccess, PgRowSeqAccess};
use serde::de::Error as _;
use serde::de::{value::Error as DeError, Deserialize, Deserializer, Visitor};
use serde::forward_to_deserialize_any;
use sqlx::postgres::{PgRow, PgValueRef};
use sqlx::{Row, TypeInfo, ValueRef};

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

        match type_name {
            last => {
                println!("In fallback (PgRowDeserializer): last is {last}");

                // Direct all "basic" types down to `PgValueDeserializer`
                let deserializer = PgValueDeserializer { value: raw_value };

                deserializer.deserialize_any(visitor)
            }
        }
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
        let raw_value = self.row.try_get_raw(self.index).map_err(DeError::custom)?;
        let type_info = raw_value.type_info();
        let type_name = type_info.name();

        match type_name {
            "TEXT[]" | "VARCHAR[]" => {
                let seq_access = PgArraySeqAccess::<String>::new(raw_value);
                visitor.visit_seq(seq_access)
            }
            "INT4[]" => {
                println!("INT4 found!");
                let seq_access = PgArraySeqAccess::<i32>::new(raw_value);
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
pub struct PgValueDeserializer<'a> {
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
            // Floating point numbers (using official types)
            "FLOAT4" => visitor.visit_f32(decode_raw_pg::<f32>(self.value)),
            "FLOAT8" | "NUMERIC" => visitor.visit_f64(decode_raw_pg::<f64>(self.value)),
            // 64-bit signed integers
            "INT8" => visitor.visit_i64(decode_raw_pg::<i64>(self.value)),
            // 32-bit signed integers
            "INT4" => visitor.visit_i32(decode_raw_pg::<i32>(self.value)),
            // 16-bit signed integers
            "INT2" => visitor.visit_i16(decode_raw_pg::<i16>(self.value)),
            // Boolean values
            "BOOL" => visitor.visit_bool(decode_raw_pg::<bool>(self.value)),
            // Date type: convert to string and pass to visitor
            "DATE" => {
                let date_str = decode_raw_pg::<chrono::NaiveDate>(self.value).to_string();
                visitor.visit_string(date_str)
            }
            // Time types: convert to string
            "TIME" | "TIMETZ" => {
                let time_str = decode_raw_pg::<chrono::NaiveTime>(self.value).to_string();
                visitor.visit_string(time_str)
            }
            // Timestamp types: convert to RFC 3339 string
            "TIMESTAMP" | "TIMESTAMPTZ" => {
                let ts_str =
                    decode_raw_pg::<chrono::DateTime<chrono::FixedOffset>>(self.value).to_rfc3339();
                visitor.visit_string(ts_str)
            }
            // UUID: convert to string
            "UUID" => {
                let uuid_str = decode_raw_pg::<uuid::Uuid>(self.value).to_string();
                visitor.visit_string(uuid_str)
            }
            // Binary data: BYTEA
            "BYTEA" => visitor.visit_bytes(decode_raw_pg::<&[u8]>(self.value)),
            // Interval type: convert to chrono
            "INTERVAL" => {
                let pg_interval = decode_raw_pg::<sqlx::postgres::types::PgInterval>(self.value);
                // Convert microseconds to seconds and nanoseconds.
                let secs = pg_interval.microseconds / 1_000_000;
                let nanos = (pg_interval.microseconds % 1_000_000) * 1000;
                // Convert days to duration (ignoring months)
                let days_duration = chrono::Duration::days(pg_interval.days as i64);
                let duration = chrono::Duration::seconds(secs)
                    + chrono::Duration::nanoseconds(nanos)
                    + days_duration;

                visitor.visit_string(duration.to_string())
            }
            "CHAR" | "TEXT" => visitor.visit_string(decode_raw_pg::<String>(self.value)),
            "JSON" | "JSONB" => {
                let value: serde_json::Value =
                    serde_json::from_str(decode_raw_pg::<&str>(self.value))
                        .map_err(DeError::custom)?;

                let json_map = JsonValueMapAccess::new(value).map_err(DeError::custom)?;
                visitor.visit_map(json_map)
            }
            // Fallback: decode as string
            last => {
                println!("In fallback (PgValueDeserializer): last is {last}");
                let as_string = decode_raw_pg::<String>(self.value);
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

/// Decode a raw Postgres value into a type T using sqlx’s Decode.
/// On error, log and return T::default().
fn decode_raw_pg<'a, T>(raw_value: PgValueRef<'a>) -> T
where
    T: sqlx::Decode<'a, sqlx::Postgres> + Default,
{
    match T::decode(raw_value) {
        Ok(v) => v,
        Err(e) => {
            log::error!(
                "Failed to decode {} value: {:?}",
                std::any::type_name::<T>(),
                e
            );
            T::default()
        }
    }
}
