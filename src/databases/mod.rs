use serde::de::{value::Error as DeError, Visitor};
use sqlx::Row as _;

use crate::{
    deserializers::{RowDeserializer, ValueDeserializer},
    seq_access::RowSeqAccess,
};

pub mod mysql;
pub mod postgres;

pub trait Database
where
    Self: sqlx::Database,
{
    /// Attempts to deserialize a JSON from a ValueRef
    ///
    /// If the type is not JSON then it will return `Ok(None)`. If the the type is JSON then it
    /// will parse it into a `serde_json::Value`.
    ///
    /// This function is used when attempting to deserialize a struct from a Row. Meaning JSON
    /// columns can be directly deserialized into a struct.
    fn deserialize_json<'a>(
        val_ref: <Self as sqlx::Database>::ValueRef<'a>,
    ) -> Result<Option<serde_json::Value>, DeError>;

    /// Some databases have different ways of dealing with sequences, this function will be given a
    /// type name and using that will determine if it is a sequence that should be handled by the
    /// sequence parser. (This is mainly used with postgres so is false by default).
    fn is_sequence(_type_name: &str) -> bool {
        false
    }

    /// This function is called when deserializing a sequence from a row.
    ///
    /// This is a separate function as Postgres handles sequences differently to other databases.
    fn deserialize_seq<'de, 'a, V: Visitor<'de>>(
        visitor: V,
        row_deserializer: RowDeserializer<'a, Self>,
    ) -> Result<V::Value, DeError>
    where
        usize: sqlx::ColumnIndex<<Self as sqlx::Database>::Row>,
    {
        let num_cols = row_deserializer.row.columns().len();
        let seq_access = RowSeqAccess {
            deserializer: row_deserializer,
            num_cols,
        };

        visitor.visit_seq(seq_access)
    }

    // /// This function is called for the ValueDeserializer when attempting to
    // /// deserialze a bool. This is because MySQL will often treat booleans as
    // /// TINYINTs and need to be dealt with separately
    // fn deserialize_bool<'de, 'a, V: Visitor<'de>>(
    //     deserializer: ValueDeserializer<'a, Self>,
    //     visitor: V,
    // ) -> Result<V::Value, DeError> {
    //     deserializer.deserialize_any(visitor)
    // }

    /// Takes a `ValueRef` and a visitor and will parse it depending on it's name.
    ///
    /// This is the most important function to define for each database.
    fn deserialize_value<'de, 'a, V: Visitor<'de>>(
        deserializer: ValueDeserializer<'a, Self>,
        visitor: V,
        // val_ref: <Self as sqlx::Database>::ValueRef<'a>,
    ) -> Result<V::Value, DeError>;
}
