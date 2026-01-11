use json::PgJson;
use seq::PgArraySeqAccess;
use serde::{
    de::{value::Error as DeError, Error as _, IntoDeserializer, Visitor},
    Deserializer,
};
use sqlx::{Row as _, TypeInfo as _, ValueRef as _};

mod json;
mod seq;

use crate::{decode_raw, deserializers::ValueDeserializer, seq_access::RowSeqAccess};

use super::Database;

impl Database for sqlx::Postgres {
    fn deserialize_json<'a>(
        val_ref: <Self as sqlx::Database>::ValueRef<'a>,
    ) -> Result<Option<serde_json::Value>, DeError> {
        let type_info = val_ref.type_info();
        let type_name = type_info.name();

        if !(type_name == "JSON" || type_name == "JSONB") {
            return Ok(None);
        }

        let value = decode_raw::<PgJson, sqlx::Postgres>(val_ref)
            .map_err(|err| DeError::custom(format!("Failed to decode JSON/JSONB: {err}")))?;

        Ok(Some(value.0))
    }

    fn is_sequence(type_name: &str) -> bool {
        type_name.ends_with("[]")
    }

    fn deserialize_seq<'de, 'a, V: Visitor<'de>>(
        visitor: V,
        row_deserializer: crate::deserializers::RowDeserializer<'a, Self>,
    ) -> Result<V::Value, DeError>
    where
        usize: sqlx::ColumnIndex<<Self as sqlx::Database>::Row>,
    {
        let raw_value = row_deserializer
            .row
            .try_get_raw(row_deserializer.index)
            .map_err(DeError::custom)?;
        let type_info = raw_value.type_info();
        let type_name = type_info.name();

        match type_name {
            "TEXT[]" | "VARCHAR[]" => {
                let seq_access = PgArraySeqAccess::<String>::new(raw_value)?;
                visitor.visit_seq(seq_access)
            }
            "INT4[]" => {
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
                let num_cols = row_deserializer.row.columns().len();
                let seq_access = RowSeqAccess {
                    deserializer: row_deserializer,
                    num_cols,
                };

                visitor.visit_seq(seq_access)
            }
        }
    }

    fn deserialize_value<'de, 'a, V: Visitor<'de>>(
        deserializer: ValueDeserializer<'a, Self>,
        visitor: V,
    ) -> Result<V::Value, DeError> {
        let val_ref = deserializer.value;
        let type_info = val_ref.type_info();
        let type_name = type_info.name();

        match type_name {
            "FLOAT4" => {
                let v = decode_raw::<f32, Self>(val_ref)?;
                visitor.visit_f32(v)
            }
            "FLOAT8" => {
                let v = decode_raw::<f64, Self>(val_ref)?;
                visitor.visit_f64(v)
            }
            "NUMERIC" => {
                let numeric = decode_raw::<rust_decimal::Decimal, Self>(val_ref)?;

                let num: f64 = numeric
                    .try_into()
                    .map_err(|_| DeError::custom("Failed to parse Decimal as f64"))?;

                visitor.visit_f64(num)
            }
            "INT8" => {
                let v = decode_raw::<i64, Self>(val_ref)?;
                visitor.visit_i64(v)
            }
            "INT4" => {
                let v = decode_raw::<i32, Self>(val_ref)?;
                visitor.visit_i32(v)
            }
            "INT2" => {
                let v = decode_raw::<i16, Self>(val_ref)?;
                visitor.visit_i16(v)
            }
            "BOOL" => {
                let v = decode_raw::<bool, Self>(val_ref)?;
                visitor.visit_bool(v)
            }
            "DATE" => {
                let date = decode_raw::<chrono::NaiveDate, Self>(val_ref)?;
                visitor.visit_string(date.to_string())
            }
            "TIME" | "TIMETZ" => {
                let time = decode_raw::<chrono::NaiveTime, Self>(val_ref)?;
                visitor.visit_string(time.to_string())
            }
            "TIMESTAMP" | "TIMESTAMPTZ" => {
                let ts = decode_raw::<chrono::DateTime<chrono::FixedOffset>, Self>(val_ref)?;
                visitor.visit_string(ts.to_rfc3339())
            }
            "UUID" => {
                let uuid = decode_raw::<uuid::Uuid, Self>(val_ref)?;
                visitor.visit_string(uuid.to_string())
            }
            "BYTEA" => {
                let bytes = decode_raw::<&[u8], Self>(val_ref)?;
                visitor.visit_bytes(bytes)
            }
            "INTERVAL" => {
                let pg_interval = decode_raw::<sqlx::postgres::types::PgInterval, Self>(val_ref)?;
                let secs = pg_interval.microseconds / 1_000_000;
                let nanos = (pg_interval.microseconds % 1_000_000) * 1000;
                let days_duration = chrono::Duration::days(pg_interval.days as i64);
                let duration = chrono::Duration::seconds(secs)
                    + chrono::Duration::nanoseconds(nanos)
                    + days_duration;
                visitor.visit_string(duration.to_string())
            }
            "CHAR" | "TEXT" => {
                let s = decode_raw::<String, Self>(val_ref)?;
                visitor.visit_string(s)
            }
            "JSON" | "JSONB" => {
                let value = decode_raw::<PgJson, Self>(val_ref)?;

                value.into_deserializer().deserialize_any(visitor)
            }
            _other => {
                let as_string = decode_raw::<String, Self>(val_ref)?;
                visitor.visit_string(as_string)
            }
        }
    }
}
