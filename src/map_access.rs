use serde::de::{self, value::Error as DeError, IntoDeserializer, MapAccess};
use serde::ser::Error as _;

use sqlx::{Column, Row};

use crate::{PgRowDeserializer, PgValueDeserializer};

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
