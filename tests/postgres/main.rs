use sqlx::{postgres::PgPoolOptions, PgPool};
use tokio::sync::OnceCell;

mod test_01_strings;
mod test_02_bool;
mod test_03_integer;
mod test_04_float;
mod test_05_newtype_over_primitive;
mod test_06_structs_with_prim_fields;
mod test_07_structs_with_struct_fields;
mod test_08_tuples_and_tuple_structs;
mod test_09_pg_array_as_vec;
mod test_10_same_type_columns_into_hashmap;
mod test_11_record_with_flatten;
mod test_12_structs_from_json;
mod test_13_struct;

#[allow(unused)]
pub async fn fetch_one<T: for<'de> serde::Deserialize<'de>>(query: &str) -> anyhow::Result<T> {
    let conn = conn().await;

    let row = sqlx::query(query).fetch_one(&conn).await.unwrap();

    serde_sqlx::from_row::<sqlx::Postgres, _>(row).map_err(Into::into)
}

#[allow(unused)]
pub async fn fetch_all<T: for<'de> serde::Deserialize<'de>>(query: &str) -> anyhow::Result<Vec<T>> {
    let conn = conn().await;

    let row = sqlx::query(query).fetch_all(&conn).await.unwrap();
    let result: Result<Vec<_>, _> = row
        .into_iter()
        .map(serde_sqlx::from_row::<sqlx::Postgres, _>)
        .collect();

    result.map_err(Into::into)
}

#[allow(unused)]
pub async fn fetch_optional<T: for<'de> serde::Deserialize<'de>>(
    query: &str,
) -> anyhow::Result<Option<T>> {
    let conn = conn().await;

    let row = sqlx::query(query).fetch_optional(&conn).await.unwrap();

    row.map(|row| serde_sqlx::from_row::<sqlx::Postgres, _>(row))
        .transpose()
        .map_err(Into::into)
}

async fn conn() -> PgPool {
    static CONN: OnceCell<PgPool> = OnceCell::const_new();

    async fn init() -> PgPool {
        let conn_string = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        PgPoolOptions::new().connect(&conn_string).await.unwrap()
    }

    CONN.get_or_init(init).await.clone()
}
