use std::ops::Deref;

use serde::de::Error as _;
use serde::de::{
    self, value::Error as DeError, Deserialize, Deserializer, IntoDeserializer, MapAccess, Visitor,
};
use serde::forward_to_deserialize_any;
use serde_json::Value as JsonValue;
use sqlx::postgres::{PgColumn, PgRow, PgValueRef};
use sqlx::{Column, Row, TypeInfo, ValueRef};

/// Convenience function: deserialize a PgRow into any T that implements Deserialize
pub fn from_pg_row<T>(row: PgRow) -> Result<T, DeError>
where
    T: for<'de> Deserialize<'de>,
{
    let deserializer = PgRowDeserializer::new(&row);
    T::deserialize(deserializer)
}

pub struct PgRowDeserializer<'a> {
    row: &'a PgRow,
}

impl<'a> PgRowDeserializer<'a> {
    pub fn new(row: &'a PgRow) -> Self {
        PgRowDeserializer { row }
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

    // For “any” type we delegate to our map implementation.
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.row.columns().len() {
            0 => return visitor.visit_unit(),
            1 => {}
            _ => unimplemented!("deserialize_any (many columns: {:#?})", self.row.columns()),
        };

        let raw_value = self.row.try_get_raw(0).map_err(DeError::custom)?;
        let type_info = raw_value.type_info();
        let type_name = type_info.name();

        if raw_value.is_null() {
            return visitor.visit_none();
        }

        match type_name {
            // Floating point numbers (using official types)
            "FLOAT4" => visitor.visit_f32(decode_raw_pg::<f32>(raw_value)),
            "FLOAT8" | "NUMERIC" => visitor.visit_f64(decode_raw_pg::<f64>(raw_value)),
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
                println!("In fallback: last is {last}");
                let as_string = decode_raw_pg::<String>(raw_value);
                visitor.visit_string(as_string)

                // visitor.visit_some(deserializer)
            }
        }
    }

    /// We treat the row as a map (each column is a key/value pair)
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(PgRowMapAccess {
            row: self.row,
            index: 0,
            num_cols: self.row.columns().len(),
        })
    }

    // For other types, forward to deserialize_any.
    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
        bytes byte_buf unit unit_struct newtype_struct seq tuple
        tuple_struct struct enum identifier ignored_any
    }
}

/// A MapAccess implementation that iterates over the row’s columns.
struct PgRowMapAccess<'a> {
    row: &'a PgRow,
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
            let col_name = self.row.columns()[self.index].name();
            // Use the column name as the key.
            seed.deserialize(col_name.into_deserializer()).map(Some)
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        let col = &self.row.columns()[self.index];
        let value = pgsql_to_json(self.row, col);
        self.index += 1;
        seed.deserialize(value.into_deserializer())
            .map_err(DeError::custom)
    }
}

/// Convert a PgRow column into a serde_json::Value.
pub fn pgsql_to_json(row: &PgRow, col: &PgColumn) -> JsonValue {
    // Use try_get_raw to get a raw value.
    let raw_value_result = row.try_get_raw(col.ordinal());
    match raw_value_result {
        Ok(raw_value) if !raw_value.is_null() => {
            // We take the raw value and convert it (see below).
            let mut raw_value = Some(raw_value);
            sql_nonnull_to_json_pg(|| {
                raw_value
                    .take()
                    .unwrap_or_else(|| row.try_get_raw(col.ordinal()).unwrap())
            })
        }
        Ok(_null) => JsonValue::Null,
        Err(e) => {
            log::warn!(
                "Unable to extract value from row for column `{}`: {:?}",
                col.name(),
                e
            );
            JsonValue::Null
        }
    }
}

/// Decode a non-null Postgres value into a JsonValue by matching on its type name.
/// This match arm covers common Postgres types.
fn sql_nonnull_to_json_pg<'r>(mut get_ref: impl FnMut() -> PgValueRef<'r>) -> JsonValue {
    let raw_value = get_ref();
    let type_info = raw_value.type_info();
    let type_name = type_info.name();

    match type_name {
        // Floating point numbers (NUMERIC/DECIMAL are decoded as f64)
        "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "NUMERIC" | "DECIMAL" => {
            decode_raw_pg::<f64>(raw_value).into()
        }
        // 64-bit signed integers
        "INT8" | "BIGINT" | "SERIAL8" | "BIGSERIAL" | "IDENTITY" | "INT64" | "INTEGER8"
        | "BIGINT SIGNED" => decode_raw_pg::<i64>(raw_value).into(),
        // 32-bit signed integers
        "INT" | "INT4" | "INTEGER" | "MEDIUMINT" | "YEAR" => decode_raw_pg::<i32>(raw_value).into(),
        // 16-bit signed integers
        "INT2" | "SMALLINT" | "TINYINT" => decode_raw_pg::<i16>(raw_value).into(),
        // Boolean values
        "BOOL" | "BOOLEAN" => decode_raw_pg::<bool>(raw_value).into(),
        // Date and time types (convert to strings)
        "DATE" => decode_raw_pg::<chrono::NaiveDate>(raw_value)
            .to_string()
            .into(),
        "TIME" | "TIMETZ" => decode_raw_pg::<chrono::NaiveTime>(raw_value)
            .to_string()
            .into(),
        "TIMESTAMP" | "TIMESTAMPTZ" => {
            decode_raw_pg::<chrono::DateTime<chrono::FixedOffset>>(raw_value)
                .to_rfc3339()
                .into()
        }
        // UUID (convert to string)
        "UUID" => decode_raw_pg::<uuid::Uuid>(raw_value).to_string().into(),
        // Binary data: BYTEA
        "BYTEA" => decode_raw_pg::<Vec<u8>>(raw_value).into(),
        // Interval types (Postgres returns intervals as a custom type; here we convert to string)
        // "INTERVAL" => decode_raw_pg::<sqlx::postgres::types::PgInterval>(raw_value).to_string().into(),
        // JSON types are decoded directly as a serde_json::Value.
        "JSON" | "JSONB" => decode_raw_pg::<JsonValue>(raw_value),
        // Fallback: decode as string
        _ => decode_raw_pg::<String>(raw_value).into(),
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
