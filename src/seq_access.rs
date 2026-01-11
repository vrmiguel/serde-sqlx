use serde::de::{value::Error as DeError, DeserializeSeed, SeqAccess};
use serde::ser::Error as _;
use sqlx::Row;

use crate::databases::Database;
use crate::deserializers::{RowDeserializer, ValueDeserializer};

pub(crate) struct RowSeqAccess<'a, DB: Database> {
    pub(crate) deserializer: RowDeserializer<'a, DB>,
    pub(crate) num_cols: usize,
}

impl<'de, 'a, DB: Database> SeqAccess<'de> for RowSeqAccess<'a, DB>
where
    usize: sqlx::ColumnIndex<<DB as sqlx::Database>::Row>,
{
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

            // Create a ValueDeserializer for the current column.
            let value_deserializer = ValueDeserializer::<'_, DB>::new(value);

            self.deserializer.index += 1;

            // Deserialize the value and return it wrapped in Some.
            seed.deserialize(value_deserializer).map(Some)
        } else {
            Ok(None)
        }
    }
}
