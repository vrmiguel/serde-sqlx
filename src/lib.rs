use std::ops::Deref;

use serde::de::Error as _;
use serde::de::{
    self, value::Error as DeError, Deserialize, Deserializer, IntoDeserializer, MapAccess, Visitor,
};
use serde::forward_to_deserialize_any;

use sqlx::postgres::{PgColumn, PgRow, PgValueRef};
use sqlx::{Column, Row, TypeInfo, ValueRef};

/// Deserialize a `sqlx::PgRow` into any T that implements Deserialize
pub fn from_pg_row<T>(row: PgRow) -> Result<T, DeError>
where
    T: for<'de> Deserialize<'de>,
{
    let deserializer = PgRowDeserializer::new(&row);
    T::deserialize(deserializer)
}

/// PgRowDeserializer is the "outer" deserializer, for maps and sequences
/// PgTypeDeserializer is the "inner" deserializer. I should come up with better names for each
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

#[derive(Clone)]
pub struct PgTypeDeserializer<'a> {
    outer: PgRowDeserializer<'a>,
    value: PgValueRef<'a>,
}

impl<'a> PgTypeDeserializer<'a> {
    pub fn new(outer: PgRowDeserializer<'a>, value: PgValueRef<'a>) -> Self {
        PgTypeDeserializer { outer, value }
    }
}

impl<'de, 'a> Deserializer<'de> for PgTypeDeserializer<'a> {
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
        let type_info = self.value.type_info();
        let type_name = type_info.name();

        if self.value.is_null() {
            return visitor.visit_none();
        }

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
            // JSON types: decode as map
            // TODO: maybe this should not be here, only in the bigger Deserializer
            "CHAR" | "TEXT" => visitor.visit_string(decode_raw_pg::<String>(self.value)),
            // Fallback: decode as string
            last => {
                println!("In fallback (PgTypeDeserializer): last is {last}");
                let as_string = decode_raw_pg::<String>(self.value);
                visitor.visit_string(as_string)

                // visitor.visit_some(deserializer)
            }
        }
    }

    // For other types, forward to deserialize_any.
    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
        bytes byte_buf unit unit_struct struct map newtype_struct seq tuple
        tuple_struct enum identifier ignored_any
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
            _ => unimplemented!("deserialize_any (many columns: {:#?})", self.row.columns()),
        };

        let column = self.row.column(0);
        let raw_value = self.row.try_get_raw(0).map_err(DeError::custom)?;
        let type_info = raw_value.type_info();
        let type_name = type_info.name();

        if raw_value.is_null() {
            return visitor.visit_none();
        }

        let pg_type_deserializer = PgTypeDeserializer::new(self, column, raw_value.clone());

        match type_name {
            // Floating point numbers (using official types)
            // 64-bit signed integers
            "INT8" => visitor.visit_i64(decode_raw_pg::<i64>(raw_value)),
            // 32-bit signed integers
            "INT4" => visitor.visit_i32(decode_raw_pg::<i32>(raw_value)),
            // 16-bit signed integers
            "INT2" => visitor.visit_i16(decode_raw_pg::<i16>(raw_value)),
            // Boolean values
            "BOOL" => visitor.visit_bool(decode_raw_pg::<bool>(raw_value)),
            // Date type: convert to string and pass to visitor
            "DATE" => {
                let date_str = decode_raw_pg::<chrono::NaiveDate>(raw_value).to_string();
                visitor.visit_string(date_str)
            }
            // Time types: convert to string
            "TIME" | "TIMETZ" => {
                let time_str = decode_raw_pg::<chrono::NaiveTime>(raw_value).to_string();
                visitor.visit_string(time_str)
            }
            // Timestamp types: convert to RFC 3339 string
            "TIMESTAMP" | "TIMESTAMPTZ" => {
                let ts_str =
                    decode_raw_pg::<chrono::DateTime<chrono::FixedOffset>>(raw_value).to_rfc3339();
                visitor.visit_string(ts_str)
            }
            // UUID: convert to string
            "UUID" => {
                let uuid_str = decode_raw_pg::<uuid::Uuid>(raw_value).to_string();
                visitor.visit_string(uuid_str)
            }
            // Binary data: BYTEA
            "BYTEA" => visitor.visit_bytes(decode_raw_pg::<&[u8]>(raw_value)),
            // Interval type: convert to chrono
            "INTERVAL" => {
                let pg_interval = decode_raw_pg::<sqlx::postgres::types::PgInterval>(raw_value);
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
            // JSON types: decode as map
            "JSON" | "JSONB" => self.deserialize_map(visitor),
            "CHAR" | "TEXT" => visitor.visit_string(decode_raw_pg::<String>(raw_value)),
            // Fallback: decode as string
            last => {
                println!("In fallback (PgRowDeserializer): last is {last}");
                pg_type_deserializer.deserialize_any(visitor)
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
            index: 0,
            num_cols: self.row.columns().len(),
        })
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
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

/// A MapAccess implementation that iterates over the row’s columns.
struct PgRowMapAccess<'a> {
    deserializer: PgRowDeserializer<'a>,
    index: usize,
    num_cols: usize,
}

impl<'de, 'a> MapAccess<'de> for PgRowMapAccess<'a> {
    type Error = DeError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: de::DeserializeSeed<'de>,
    {
        if self.index < self.num_cols {
            let col_name = self.deserializer.row.columns()[self.index].name();
            // Use the column name as the key
            seed.deserialize(col_name.into_deserializer()).map(Some)
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        let column = &self.deserializer.row.columns()[self.index];
        let raw_value = self
            .deserializer
            .row
            .try_get_raw(column.ordinal())
            .map_err(DeError::custom)?;
        let deserializer = PgTypeDeserializer::new(self.deserializer, column, raw_value);

        self.index += 1;
        seed.deserialize(deserializer).map_err(DeError::custom)
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
