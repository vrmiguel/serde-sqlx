use serde::Deserialize;
use sqlx::{postgres::PgPoolOptions, PgPool};
use tokio::sync::OnceCell;

pub async fn fetch_one<T: for<'de> serde::Deserialize<'de>>(query: &str) -> anyhow::Result<T> {
    let conn = conn().await;

    let row = sqlx::query(query).fetch_one(&conn).await.unwrap();

    serde_sqlx::from_pg_row(row).map_err(Into::into)
}

pub async fn fetch_all<T: for<'de> serde::Deserialize<'de>>(query: &str) -> anyhow::Result<Vec<T>> {
    let conn = conn().await;

    let row = sqlx::query(query).fetch_all(&conn).await.unwrap();
    let result: Result<Vec<_>, _> = row.into_iter().map(serde_sqlx::from_pg_row).collect();

    result.map_err(Into::into)
}

pub async fn fetch_optional<T: for<'de> serde::Deserialize<'de>>(
    query: &str,
) -> anyhow::Result<Option<T>> {
    let conn = conn().await;

    let row = sqlx::query(query).fetch_optional(&conn).await.unwrap();

    row.map(|row| serde_sqlx::from_pg_row(row))
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

#[tokio::test]
async fn single_json_field_into_a_record_field() -> anyhow::Result<()> {
    #[derive(Deserialize, Debug, PartialEq)]
    struct Inner {
        one: i32,
        two: i32,
        three: i32,
    }

    #[derive(Deserialize, Debug, PartialEq)]
    struct JsonRecord {
        json_record: Inner,
    }

    let out: Vec<JsonRecord> = fetch_all(
        r#"
            SELECT '{"one": 1, "two": 2, "three": 3}' :: JSON json_record
        "#,
    )
    .await?;

    assert_eq!(
        out,
        vec![JsonRecord {
            json_record: Inner {
                one: 1,
                two: 2,
                three: 3
            }
        }]
    );

    Ok(())
}
