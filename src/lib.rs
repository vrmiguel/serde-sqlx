use databases::Database;
use deserializers::RowDeserializer;
use serde::de::{value::Error as DeError, Deserialize};
use serde::de::{DeserializeOwned, Error};

use sqlx::postgres::PgRow;

mod databases;
mod deserializers;
mod map_access;
mod seq_access;

/// Convenience function to deserialize a generic `sqlx::Row` into a serde Deserializable `T`
pub fn from_row<DB, T>(row: <DB as sqlx::Database>::Row) -> Result<T, DeError>
where
    DB: Database,
    usize: sqlx::ColumnIndex<<DB as sqlx::Database>::Row>,
    T: DeserializeOwned,
{
    let deserializer: RowDeserializer<'_, DB> = RowDeserializer::new(&row);
    T::deserialize(deserializer)
}

/// Convenience function: deserialize a PgRow into any T that implements Deserialize
#[deprecated = "Use the more generic `from_row` function instead"]
pub fn from_pg_row<T>(row: PgRow) -> Result<T, DeError>
where
    T: for<'de> Deserialize<'de>,
{
    from_row::<sqlx::Postgres, T>(row)
}

fn decode_raw<'a, T, DB>(raw_value: <DB as sqlx::Database>::ValueRef<'a>) -> Result<T, DeError>
where
    DB: sqlx::Database,
    T: sqlx::Decode<'a, DB>,
{
    T::decode(raw_value).map_err(|err| {
        DeError::custom(format!(
            "Failed to decode {} value: {:?}",
            std::any::type_name::<T>(),
            err,
        ))
    })
}
