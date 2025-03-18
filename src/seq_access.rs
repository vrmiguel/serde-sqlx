use serde::de::{value::Error as DeError, DeserializeSeed, SeqAccess};
use serde::ser::Error as _;
use sqlx::postgres::PgValueRef;
use sqlx::Row;

use crate::{decode_raw_pg, PgRowDeserializer, PgValueDeserializer};

/// A SeqAccess implementation that iterates over the row’s columns.
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

/// A custom SeqAccess implementation for PostgreSQL arrays.
/// It decodes a raw PostgreSQL array (e.g. TEXT[]) into a `Vec<T>` and
/// then yields each element during deserialization.
pub struct PgArraySeqAccess<T> {
    iter: std::vec::IntoIter<T>,
}

impl<'de, 'a, T> PgArraySeqAccess<T>
where
    T: sqlx::Decode<'a, sqlx::Postgres>,
{
    /// Creates a new `PgArraySeqAccess` from a raw PostgreSQL array value.
    /// The raw value is decoded using `decode_raw_pg::<Vec<T>>(raw)`.
    pub fn new(value: PgValueRef<'a>) -> Self
    where
        Vec<T>: sqlx::Decode<'a, sqlx::Postgres>,
    {
        // Call your existing decoding function.
        let vec = decode_raw_pg::<Vec<T>>(value);
        println!("Deserialized vec in PgArraySeqAccess");
        PgArraySeqAccess {
            iter: vec.into_iter(),
        }
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
        if let Some(value) = self.iter.next() {
            seed.deserialize(value.into_deserializer())
                .map(Some)
                .map_err(|err| {
                    println!("WAAAAA");
                    err
                })
        } else {
            Ok(None)
        }
    }
}
