use serde::{
    de::{
        self, value::Error as DeError, DeserializeSeed, Error as _, IntoDeserializer, SeqAccess,
        Visitor,
    },
    forward_to_deserialize_any,
};
use sqlx::postgres::PgValueRef;

use crate::decode_raw;

/// SeqAccess implementation for Postgres arrays
/// It decodes a raw Postgres array, such as TEXT[] into a `Vec<Option<T>>` and
/// then yields each element during deserialization
pub struct PgArraySeqAccess<T> {
    iter: std::vec::IntoIter<Option<T>>,
}

impl<'a, T> PgArraySeqAccess<T>
where
    T: sqlx::Decode<'a, sqlx::Postgres> + std::fmt::Debug,
{
    pub fn new(value: PgValueRef<'a>) -> Result<Self, DeError>
    where
        Vec<Option<T>>: sqlx::Decode<'a, sqlx::Postgres> + std::fmt::Debug,
    {
        let vec: Vec<Option<T>> = decode_raw(value)?;

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
