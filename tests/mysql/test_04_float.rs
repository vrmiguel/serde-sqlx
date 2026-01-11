use crate::{fetch_all, fetch_one};

#[tokio::test]
async fn unannotated_as_f32() {
    let row: f32 = fetch_one("SELECT -4.2 AS value").await.unwrap();
    assert_eq!(row, -4.2);
}

#[tokio::test]
async fn unannotated_as_f64() {
    let row: f64 = fetch_one("SELECT -4.2 AS value").await.unwrap();
    assert_eq!(row, -4.2);
}

#[tokio::test]
async fn cast_float_as_f32() {
    let row: f32 = fetch_one("SELECT CAST(-4.2 AS FLOAT) AS value")
        .await
        .unwrap();
    assert_eq!(row, -4.2);
}

#[tokio::test]
async fn cast_double_as_f64() {
    let row: f64 = fetch_one("SELECT CAST(-4.2 AS DOUBLE) AS value")
        .await
        .unwrap();
    assert_eq!(row, -4.2);
}

#[tokio::test]
async fn cast_decimal_as_f64() {
    let row: f64 = fetch_one("SELECT CAST(-4.2 AS DECIMAL(10, 5)) AS value")
        .await
        .unwrap();
    assert_eq!(row, -4.2);
}

#[tokio::test]
async fn unannotated_as_f32_opt() {
    let row: Vec<Option<f32>> = fetch_all("SELECT -4.2 AS value UNION ALL SELECT NULL")
        .await
        .unwrap();
    assert_eq!(row, vec![Some(-4.2), None]);
}

#[tokio::test]
async fn unannotated_as_f64_opt() {
    let row: Vec<Option<f64>> = fetch_all("SELECT -4.2 AS value UNION ALL SELECT NULL")
        .await
        .unwrap();
    assert_eq!(row, vec![Some(-4.2), None]);
}

#[tokio::test]
async fn cast_float_as_f32_opt() {
    let row: Vec<Option<f32>> =
        fetch_all("SELECT CAST(-4.2 AS FLOAT) AS value UNION ALL SELECT NULL")
            .await
            .unwrap();
    assert_eq!(row, vec![Some(-4.2), None]);
}

#[tokio::test]
async fn cast_double_as_f64_opt() {
    let row: Vec<Option<f64>> =
        fetch_all("SELECT CAST(-4.2 AS DOUBLE) AS value UNION ALL SELECT NULL")
            .await
            .unwrap();
    assert_eq!(row, vec![Some(-4.2), None]);
}

#[tokio::test]
async fn cast_decimal_as_f64_opt() {
    let row: Vec<Option<f64>> =
        fetch_all("SELECT CAST(-4.2 AS DECIMAL(10, 5)) AS value UNION ALL SELECT NULL")
            .await
            .unwrap();
    assert_eq!(row, vec![Some(-4.2), None]);
}
