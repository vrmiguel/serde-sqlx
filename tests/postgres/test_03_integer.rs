use crate::{fetch_one, fetch_optional};

#[tokio::test]
async fn int2_as_i16() {
    let row: i16 = fetch_one("SELECT 42 :: INT2 AS value").await.unwrap();
    assert_eq!(row, 42);
}

#[tokio::test]
async fn unannotated_as_i32() {
    let row: i32 = fetch_one("SELECT 42 AS value").await.unwrap();
    assert_eq!(row, 42);
}

#[tokio::test]
async fn unannotated_as_i32_opt() {
    let rows: Option<i32> = fetch_optional("SELECT 42 AS value UNION ALL SELECT NULL")
        .await
        .unwrap();

    assert_eq!(rows, Some(42));
}

#[tokio::test]
async fn int2_as_i32() {
    let row: i32 = fetch_one("SELECT 42 :: INT2 AS value").await.unwrap();
    assert_eq!(row, 42);
}

#[tokio::test]
async fn int4_as_i32() {
    let row: i32 = fetch_one("SELECT 42 :: INT4 AS value").await.unwrap();
    assert_eq!(row, 42);
}

#[tokio::test]
async fn int2_as_i64() {
    let row: i64 = fetch_one("SELECT 42 :: INT2 AS value").await.unwrap();
    assert_eq!(row, 42);
}

#[tokio::test]
async fn int4_as_i64() {
    let row: i64 = fetch_one("SELECT 42 :: INT4 AS value").await.unwrap();
    assert_eq!(row, 42);
}

#[tokio::test]
async fn int8_as_i64() {
    let row: i64 = fetch_one("SELECT 42 :: INT8 AS value").await.unwrap();
    assert_eq!(row, 42);
}

#[tokio::test]
async fn int8_as_i64_opt() {
    let row: Option<i64> = fetch_one("SELECT 42 :: INT8 AS value").await.unwrap();
    assert_eq!(row, Some(42));
}

#[tokio::test]
async fn bigint_as_i64() {
    let row: i64 = fetch_one("SELECT 42 :: BIGINT AS value").await.unwrap();
    assert_eq!(row, 42);
}
