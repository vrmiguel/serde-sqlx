use serde::Deserialize;

use crate::{fetch_all, fetch_one};

#[tokio::test]
async fn unannotated_as_bool() {
    let rows: Vec<bool> = fetch_all("SELECT true UNION ALL SELECT false")
        .await
        .unwrap();

    assert_eq!(rows, [true, false]);
}

#[tokio::test]
async fn t_unannotated_as_bool_opt() {
    let rows =
        fetch_all::<Option<bool>>("SELECT true UNION ALL SELECT false UNION ALL SELECT NULL")
            .await
            .unwrap();

    assert_eq!(rows, [Some(true), Some(false), None])
}

#[tokio::test]
async fn t_unannotated_as_bool_opt_from_int() {
    let rows =
        fetch_all::<Option<bool>>("SELECT CAST(1 AS SIGNED) UNION ALL SELECT CAST(0 AS SIGNED) UNION ALL SELECT NULL")
            .await
            .unwrap();

    assert_eq!(rows, [Some(true), Some(false), None])
}

#[tokio::test]
async fn test_integer_as_bool_true() {
    let val: bool = fetch_one("SELECT 1").await.unwrap();
    assert!(val);
}

#[tokio::test]
async fn test_integer_as_bool_false() {
    let val: bool = fetch_one("SELECT 0").await.unwrap();
    assert!(!val);
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct InnerBool {
    inner: bool,
}

#[tokio::test]
async fn test_integer_as_bool_true_inner() {
    let val: InnerBool = fetch_one("SELECT 1 AS `inner`").await.unwrap();
    assert_eq!(val, InnerBool { inner: true });
}

#[tokio::test]
async fn test_integer_as_bool_false_inner() {
    let val: InnerBool = fetch_one("SELECT 0 AS `inner`").await.unwrap();
    assert_eq!(val, InnerBool { inner: false });
}
