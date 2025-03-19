use deserializers::PgRowDeserializer;
use serde::de::{value::Error as DeError, Deserialize};
use sqlx::postgres::{PgRow, PgValueRef};

/// Convenience function: deserialize a PgRow into any T that implements Deserialize
pub fn from_pg_row<T>(row: PgRow) -> Result<T, DeError>
where
    T: for<'de> Deserialize<'de>,
{
    let deserializer = PgRowDeserializer::new(&row);
    T::deserialize(deserializer)
}

fn decode_raw_pg<'a, T>(raw_value: PgValueRef<'a>) -> Option<T>
where
    T: sqlx::Decode<'a, sqlx::Postgres>,
{
    match T::decode(raw_value) {
        Ok(v) => Some(v),
        Err(err) => {
            eprintln!(
                "Failed to decode {} value: {:?}",
                std::any::type_name::<T>(),
                err,
            );
            None
        }
    }
}

mod seq_access {
    use std::fmt::Debug;

    use serde::de::{value::Error as DeError, DeserializeSeed, SeqAccess, Visitor};
    use serde::ser::Error as _;
    use serde::{de, forward_to_deserialize_any};
    use sqlx::{postgres::PgValueRef, Row};

    use crate::{
        decode_raw_pg,
        deserializers::{PgRowDeserializer, PgValueDeserializer},
    };

    /// A SeqAccess implementation that iterates over the row’s columns
    pub(crate) struct PgRowSeqAccess<'a> {
        pub(crate) deserializer: PgRowDeserializer<'a>,
        pub(crate) num_cols: usize,
    }

    impl<'de, 'a> SeqAccess<'de> for PgRowSeqAccess<'a> {
        type Error = DeError;

        fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
        where
            T: DeserializeSeed<'de>,
        {
            if self.deserializer.index < self.num_cols {
                let value = self
                    .deserializer
                    .row
                    .try_get_raw(self.deserializer.index)
                    .map_err(DeError::custom)?;

                // Create a PgValueDeserializer for the current column.
                let pg_value_deserializer = PgValueDeserializer { value };

                println!(
                    "Deserializing sequence element at index {}",
                    self.deserializer.index
                );

                self.deserializer.index += 1;

                // Deserialize the value and return it wrapped in Some.
                seed.deserialize(pg_value_deserializer).map(Some)
            } else {
                Ok(None)
            }
        }
    }

    use serde::de::IntoDeserializer;

    /// SeqAccess implementation for Postgres arrays
    /// It decodes a raw Postgres array, such as TEXT[] into a `Vec<Option<T>>` and
    /// then yields each element during deserialization
    pub struct PgArraySeqAccess<T> {
        iter: std::vec::IntoIter<Option<T>>,
    }

    impl<'de, 'a, T> PgArraySeqAccess<T>
    where
        T: sqlx::Decode<'a, sqlx::Postgres> + Debug,
    {
        pub fn new(value: PgValueRef<'a>) -> Result<Self, DeError>
        where
            Vec<Option<T>>: sqlx::Decode<'a, sqlx::Postgres> + Debug,
        {
            let vec: Vec<Option<T>> = decode_raw_pg(value)
                .ok_or_else(|| DeError::custom("Failed to decode PostgreSQL array"))?;

            Ok(PgArraySeqAccess {
                iter: vec.into_iter(),
            })
        }
    }

    impl<'de, T> SeqAccess<'de> for PgArraySeqAccess<T>
    where
        T: IntoDeserializer<'de, DeError>,
    {
        type Error = DeError;

        fn next_element_seed<U>(&mut self, seed: U) -> Result<Option<U::Value>, Self::Error>
        where
            U: DeserializeSeed<'de>,
        {
            let Some(value) = self.iter.next() else {
                return Ok(None);
            };

            seed.deserialize(PgArrayElementDeserializer { value })
                .map(Some)
        }
    }

    /// Yet another deserializer, this time to handles Options
    struct PgArrayElementDeserializer<T> {
        pub value: Option<T>,
    }

    impl<'de, T> de::Deserializer<'de> for PgArrayElementDeserializer<T>
    where
        T: IntoDeserializer<'de, DeError>,
    {
        type Error = DeError;

        fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            match self.value {
                Some(v) => visitor.visit_some(v.into_deserializer()),
                None => visitor.visit_none(),
            }
        }

        fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            match self.value {
                Some(v) => v.into_deserializer().deserialize_any(visitor),
                None => Err(DeError::custom(
                    "unexpected null in non-optional array element",
                )),
            }
        }

        forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
            bytes byte_buf unit unit_struct newtype_struct seq tuple tuple_struct
            map struct enum identifier ignored_any
        }
    }
}

mod map_access {
    use serde::de::{self, value::Error as DeError, IntoDeserializer, MapAccess};
    use serde::ser::Error as _;

    use sqlx::{Column, Row};

    use crate::deserializers::{PgRowDeserializer, PgValueDeserializer};

    pub(crate) struct PgRowMapAccess<'a> {
        pub(crate) deserializer: PgRowDeserializer<'a>,
        pub(crate) num_cols: usize,
    }

    impl<'de, 'a> MapAccess<'de> for PgRowMapAccess<'a> {
        type Error = DeError;

        fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
        where
            K: de::DeserializeSeed<'de>,
        {
            if self.deserializer.index < self.num_cols {
                let col_name = self.deserializer.row.columns()[self.deserializer.index].name();
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
            let value = self
                .deserializer
                .row
                .try_get_raw(self.deserializer.index)
                .map_err(DeError::custom)?;
            let pg_type_deserializer = PgValueDeserializer { value };

            self.deserializer.index += 1;

            seed.deserialize(pg_type_deserializer)
        }
    }
}

mod deserializers {
    use crate::decode_raw_pg;
    use crate::json::PgJson;
    use crate::map_access::PgRowMapAccess;
    use crate::seq_access::{PgArraySeqAccess, PgRowSeqAccess};
    use serde::de::{value::Error as DeError, Deserializer, Visitor};
    use serde::de::{Error as _, IntoDeserializer};
    use serde::forward_to_deserialize_any;
    use sqlx::postgres::{PgRow, PgValueRef};
    use sqlx::{Row, TypeInfo, ValueRef};

    #[derive(Clone, Copy)]
    pub struct PgRowDeserializer<'a> {
        pub(crate) row: &'a PgRow,
        pub(crate) index: usize,
    }

    impl<'a> PgRowDeserializer<'a> {
        pub fn new(row: &'a PgRow) -> Self {
            PgRowDeserializer { row, index: 0 }
        }

        #[allow(unused)]
        pub fn is_json(&self) -> bool {
            self.row.try_get_raw(0).map_or(false, |value| {
                matches!(value.type_info().name(), "JSON" | "JSONB")
            })
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
            fields: &'static [&'static str],
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            let raw_value = self.row.try_get_raw(self.index).map_err(DeError::custom)?;
            let type_info = raw_value.type_info();
            let type_name = type_info.name();

            if type_name == "JSON" || type_name == "JSONB" {
                let value = decode_raw_pg::<PgJson>(raw_value)
                    .ok_or_else(|| DeError::custom("Failed to decode JSON/JSONB"))?;

                if let serde_json::Value::Object(ref obj) = value.0 {
                    if fields.len() == 1 {
                        // If there's only one expected field, check if the object already contains it.
                        if obj.contains_key(fields[0]) {
                            // If so, we can deserialize directly.
                            return value.into_deserializer().deserialize_any(visitor);
                        } else {
                            // Otherwise, wrap the object in a new map keyed by that field name.
                            let mut map = serde_json::Map::new();
                            map.insert(fields[0].to_owned(), value.0);
                            return map
                                .into_deserializer()
                                .deserialize_any(visitor)
                                .map_err(DeError::custom);
                        }
                    } else {
                        // For multiple expected fields, ensure the JSON object already contains all of them.
                        if fields.iter().all(|&field| obj.contains_key(field)) {
                            return value.into_deserializer().deserialize_any(visitor);
                        } else {
                            return Err(DeError::custom(format!(
                                "JSON object missing expected keys: expected {:?}, found keys {:?}",
                                fields,
                                obj.keys().collect::<Vec<_>>()
                            )));
                        }
                    }
                } else {
                    // For non-object JSON values, delegate directly.
                    return value.into_deserializer().deserialize_any(visitor);
                }
            }

            // Fallback for non-JSON types.
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
    pub(crate) struct PgValueDeserializer<'a> {
        pub(crate) value: PgValueRef<'a>,
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
                    let pg_interval =
                        decode_raw_pg::<sqlx::postgres::types::PgInterval>(self.value)
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
}

mod json {
    use serde::{
        de::{self, value::Error as DeError, Deserializer, Error, IntoDeserializer},
        forward_to_deserialize_any,
    };
    use serde_json::Value;
    use sqlx::{
        postgres::{PgTypeInfo, PgValueRef},
        Postgres, TypeInfo, ValueRef,
    };

    /// Decodes Postgres' JSON or JSONB into serde_json::Value
    #[derive(Debug)]
    pub(crate) struct PgJson(pub(crate) serde_json::Value);

    impl<'a> sqlx::Decode<'a, sqlx::Postgres> for PgJson {
        fn decode(value: PgValueRef<'a>) -> Result<Self, sqlx::error::BoxDynError> {
            let is_jsonb = match value.type_info().name() {
                "JSON" => false,
                "JSONB" => true,
                other => unreachable!("Got {other} in PgJson"),
            };

            let mut bytes = value.as_bytes()?;

            // For JSONB, the first byte is a version (should be 1)
            if is_jsonb {
                if bytes.is_empty() || bytes[0] != 1 {
                    return Err("invalid JSONB header".into());
                }

                // Skip the version byte
                bytes = &bytes[1..]
            };

            let value = serde_json::from_slice(bytes)?;

            Ok(PgJson(value))
        }
    }

    impl sqlx::Type<Postgres> for PgJson {
        fn type_info() -> PgTypeInfo {
            PgTypeInfo::with_name("JSON")
        }
    }

    pub struct PgJsonDeserializer {
        value: Value,
    }

    impl<'de> Deserializer<'de> for PgJsonDeserializer {
        type Error = DeError;

        fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: de::Visitor<'de>,
        {
            // Delegate to serde_json::Value's own Deserializer
            self.value.deserialize_any(visitor).map_err(DeError::custom)
        }

        forward_to_deserialize_any! {
            bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
            bytes byte_buf option unit unit_struct newtype_struct seq tuple
            tuple_struct map struct enum identifier ignored_any
        }
    }

    impl<'de> IntoDeserializer<'de> for PgJson {
        type Deserializer = PgJsonDeserializer;

        fn into_deserializer(self) -> Self::Deserializer {
            PgJsonDeserializer { value: self.0 }
        }
    }
}
