use crate::{fetch_all, fetch_one};

#[derive(Debug, serde::Deserialize, PartialEq, Eq)]
struct NewType<T>(T);

#[tokio::test]
async fn f64_newtype() {
    let row: NewType<f64> = fetch_one("SELECT 4.2 AS value").await.unwrap();
    assert_eq!(row, NewType(4.2));
}

#[tokio::test]
async fn unannotated_as_i32_newtype() {
    let rows: Vec<NewType<i32>> = fetch_all("SELECT 1 AS value UNION ALL SELECT 2")
        .await
        .unwrap();
    assert_eq!(rows, [NewType(1), NewType(2)]);
}
