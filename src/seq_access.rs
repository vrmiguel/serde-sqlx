use std::fmt::Debug;

use serde::de::{self, value::Error as DeError, DeserializeSeed, SeqAccess, Visitor};
use serde::de::value::NoneDeserializer;
use serde::ser::Error as _;
use serde::forward_to_deserialize_any;
use sqlx::{postgres::PgValueRef, Row};

use crate::{
    decode_raw_pg,
    deserializers::{PgRowDeserializer, PgValueDeserializer},
    json::PgJson,
};

/// A SeqAccess implementation that iterates over the row's columns
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
        let vec: Vec<Option<T>> = decode_raw_pg(value)?;

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

/// A specialized array sequence access for JSON arrays that properly converts PgJson to JsValue
pub struct PgJsonArraySeqAccess<'a> {
    iter: std::vec::IntoIter<Option<PgJson>>,
}

impl<'de, 'a> PgJsonArraySeqAccess<'a> {
    pub fn new(value: PgValueRef<'a>) -> Result<Self, DeError> {
        let vec: Vec<Option<PgJson>> = decode_raw_pg(value)?;

        Ok(PgJsonArraySeqAccess {
            iter: vec.into_iter(),
        })
    }
}

impl<'de> SeqAccess<'de> for PgJsonArraySeqAccess<'_> {
    type Error = DeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.iter.next() {
            Some(Some(pg_json)) => {
                // Convert PgJson to serde_json::Value (JsValue)
                let json_value = pg_json.0;
                seed.deserialize(json_value.into_deserializer()).map(Some)
            }
            Some(None) => seed.deserialize(NoneDeserializer::default()).map(Some),
            None => Ok(None),
        }
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