use serde::{
    de::{self, value::Error as DeError, Deserializer, Error, IntoDeserializer},
    forward_to_deserialize_any,
};
use serde_json::Value;
use sqlx::{
    postgres::{PgTypeInfo, PgValueRef},
    Postgres,
};

#[derive(Debug)]
pub(crate) struct PgJson(pub(crate) serde_json::Value);

impl<'a> sqlx::Decode<'a, sqlx::Postgres> for PgJson {
    fn decode(value: PgValueRef<'a>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <&str as sqlx::Decode<sqlx::Postgres>>::decode(value)?;

        let v = serde_json::from_str(s)?;
        Ok(PgJson(v))
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

// impl<'de, 'a> Deserializer<'de> for PgRowDeserializer<'a> {
//     type Error = DeError;

impl<'de> Deserializer<'de> for PgJsonDeserializer {
    type Error = DeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        // Delegate to serde_json::Value's own Deserializer implementation.
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
