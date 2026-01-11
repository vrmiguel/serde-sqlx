use crate::{decode_raw, deserializers::{ValueDeserializer, ValueType}};
use serde::de::{value::Error as DeError, Deserializer as _, Error, IntoDeserializer as _};
use sqlx::{TypeInfo as _, ValueRef as _};

use super::Database;

impl Database for sqlx::MySql {
    fn deserialize_json<'a>(
        val_ref: <Self as sqlx::Database>::ValueRef<'a>,
    ) -> Result<Option<serde_json::Value>, serde::de::value::Error> {
        let type_info = val_ref.type_info();
        let type_name = type_info.name();

        if type_name != "JSON" {
            return Ok(None);
        }

        let value = decode_raw::<serde_json::Value, Self>(val_ref)?;

        Ok(Some(value))
    }

    fn deserialize_value<'de, 'a, V: serde::de::Visitor<'de>>(
        deserializer: ValueDeserializer<'a, Self>,
        visitor: V,
    ) -> Result<V::Value, serde::de::value::Error> {
        let val_ref = deserializer.value;

        let type_info = val_ref.type_info();
        let type_name = type_info.name();

        // Handle enums
        if deserializer.value_type == ValueType::Enum {
            let v = decode_raw::<String, Self>(val_ref)?;
            return visitor.visit_enum(v.into_deserializer());
        }

        // Note this is pretty brittle and hacky, would love if the max_size was given in the
        // public API :/
        let max_size_one = format!("{type_info:?}").contains("max_size: Some(1)");

        // Handle booleans ahead of time as booleans in MySQL often come as an integer
        if max_size_one || deserializer.value_type == ValueType::Bool {
            let v = decode_raw::<bool, Self>(val_ref)?;
            return visitor.visit_bool(v);
        }

        match type_name {
            "TINYINT" => {
                let v = decode_raw::<i8, Self>(val_ref)?;
                visitor.visit_i8(v)
            }
            "TINYINT UNSIGNED" => {
                let v = decode_raw::<u8, Self>(val_ref)?;
                visitor.visit_u8(v)
            }
            "SMALLINT" => {
                let v = decode_raw::<i16, Self>(val_ref)?;
                visitor.visit_i16(v)
            }
            "SMALLINT UNSIGNED" => {
                let v = decode_raw(val_ref)?;
                visitor.visit_u16(v)
            }
            "INT" | "MEDIUMINT" | "YEAR" => {
                let v = decode_raw::<i32, Self>(val_ref)?;
                visitor.visit_i32(v)
            }
            "INT UNSIGNED" | "MEDIUMINT UNSIGNED" => {
                let v = decode_raw(val_ref)?;
                visitor.visit_u32(v)
            }
            "BIGINT" => {
                let v = decode_raw::<i64, Self>(val_ref)?;
                visitor.visit_i64(v)
            }
            "BIGINT UNSIGNED" => {
                let v = decode_raw(val_ref)?;
                visitor.visit_u64(v)
            }
            "FLOAT" => {
                let v = decode_raw::<f32, Self>(val_ref)?;
                visitor.visit_f32(v)
            }
            "DOUBLE" => {
                let v = decode_raw::<f64, Self>(val_ref)?;
                visitor.visit_f64(v)
            }
            "DECIMAL" => {
                let numeric = decode_raw::<rust_decimal::Decimal, Self>(val_ref)?;
                let num: f64 = numeric
                    .try_into()
                    .map_err(|_| DeError::custom("Failed to parse Decimal as f64"))?;
                visitor.visit_f64(num)
            }
            "BOOLEAN" => {
                let v = decode_raw::<bool, Self>(val_ref)?;
                visitor.visit_bool(v)
            }
            "CHAR" | "VARCHAR" | "ENUM" | "SET" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT"
            | "LONGTEXT" => {
                let v = decode_raw::<String, Self>(val_ref)?;
                visitor.visit_string(v)
            }
            "BINARY" | "BIT" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" | "VARBINARY" => {
                let v = decode_raw::<&[u8], Self>(val_ref)?;
                visitor.visit_bytes(v)
            }
            "DATE" => {
                let v = decode_raw::<chrono::NaiveDate, Self>(val_ref)?;
                visitor.visit_string(v.to_string())
            }
            "TIME" => {
                let v = decode_raw::<chrono::NaiveTime, Self>(val_ref)?;
                visitor.visit_string(v.to_string())
            }
            "DATETIME" | "TIMESTAMP" => {
                let v = decode_raw::<chrono::NaiveDateTime, Self>(val_ref)?;
                visitor.visit_string(v.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
            }
            "JSON" => {
                let value = decode_raw::<serde_json::Value, Self>(val_ref)?;
                value
                    .into_deserializer()
                    .deserialize_any(visitor)
                    .map_err(DeError::custom)
            }
            "NULL" => visitor.visit_none(),
            _other => {
                let as_string = decode_raw::<String, Self>(val_ref)?;
                visitor.visit_string(as_string)
            }
        }
    }
}
