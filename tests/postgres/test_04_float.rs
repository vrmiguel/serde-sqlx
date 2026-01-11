use crate::{fetch_all, fetch_one};

#[tokio::test]
async fn real_negative_as_f32() {
    let row: f32 = fetch_one("SELECT -4.2 :: REAL AS value").await.unwrap();
    assert_eq!(row, -4.2);
}

#[tokio::test]
async fn real_as_f32_opt() {
    let rows: Vec<f32> = fetch_all("SELECT 10.0 :: REAL AS value UNION ALL SELECT -10.0")
        .await
        .unwrap();
    assert_eq!(rows, vec![10.0, -10.0]);
}

#[tokio::test]
async fn real_nan_as_f32() {
    let row: f32 = fetch_one("SELECT 'NaN'::REAL AS value").await.unwrap();
    assert!(row.is_nan(), "Expected NaN for f32");
}

#[tokio::test]
async fn double_precision_nan_as_f64() {
    let row: f64 = fetch_one("SELECT 'NaN'::DOUBLE PRECISION AS value")
        .await
        .unwrap();
    assert!(row.is_nan(), "Expected NaN for f64");
}

#[tokio::test]
async fn double_precision_infinity_as_f64() {
    let row: f64 = fetch_one("SELECT 'Infinity'::DOUBLE PRECISION AS value")
        .await
        .unwrap();
    assert!(
        row.is_infinite() && row.is_sign_positive(),
        "Expected positive infinity"
    );
}

#[tokio::test]
async fn double_precision_negative_infinity_as_f64() {
    let row: f64 = fetch_one("SELECT '-Infinity'::DOUBLE PRECISION AS value")
        .await
        .unwrap();
    assert!(
        row.is_infinite() && row.is_sign_negative(),
        "Expected negative infinity"
    );
}
