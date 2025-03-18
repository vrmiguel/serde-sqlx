use serde::de::{self, value::Error as DeError, DeserializeSeed, IntoDeserializer, MapAccess};
use serde::ser::Error as _;
use serde_json::Value;

use sqlx::{Column, Row};

use crate::{PgRowDeserializer, PgValueDeserializer};

/// MapAccess for serde_json::Value
pub(crate) struct JsonValueMapAccess {
    iter: serde_json::map::IntoIter,
    /// Holds the value from the current key/value pair
    current: Option<Value>,
}

impl JsonValueMapAccess {
    pub fn new(json: Value) -> Result<Self, DeError> {
        match json {
            Value::Object(map) => Ok(JsonValueMapAccess {
                iter: map.into_iter(),
                current: None,
            }),
            _ => Err(DeError::custom("expected a JSON object")),
        }
    }
}

impl<'de, 'a> MapAccess<'de> for JsonValueMapAccess {
    type Error = DeError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if let Some((key, value)) = self.iter.next() {
            // Save the current entry so next_value_seed() can use it
            self.current = Some(value);

            seed.deserialize(key.into_deserializer()).map(Some)
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        // Retrieve the stored value from next_key_seed
        if let Some(value) = self.current.take() {
            seed.deserialize(value).map_err(DeError::custom)
        } else {
            Err(DeError::custom("value is missing"))
        }
    }
}

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
