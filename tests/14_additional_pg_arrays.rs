mod util;

use util::fetch_all;
use uuid::Uuid;

#[tokio::test]
async fn pg_arr_of_int8_as_vec_i64() {
    let rows: Vec<Vec<i64>> =
        fetch_all("SELECT array_agg(R.a) _0 FROM (SELECT 9223372036854775807::INT8 a UNION ALL SELECT -9223372036854775808::INT8) R")
            .await
            .unwrap();
    assert_eq!(rows, vec![vec![9223372036854775807, -9223372036854775808]]);
}

#[tokio::test]
async fn pg_arr_of_int2_as_vec_i16() {
    let rows: Vec<Vec<i16>> =
        fetch_all("SELECT array_agg(R.a) _0 FROM (SELECT 32767::INT2 a UNION ALL SELECT -32768::INT2) R")
            .await
            .unwrap();
    assert_eq!(rows, vec![vec![32767, -32768]]);
}

#[tokio::test]
async fn pg_arr_of_float4_as_vec_f32() {
    let rows: Vec<Vec<f32>> =
        fetch_all("SELECT array_agg(R.a) _0 FROM (SELECT 3.14::FLOAT4 a UNION ALL SELECT -2.71::FLOAT4) R")
            .await
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert!((rows[0][0] - 3.14).abs() < f32::EPSILON);
    assert!((rows[0][1] - (-2.71)).abs() < f32::EPSILON);
}

#[tokio::test]
async fn pg_arr_of_float8_as_vec_f64() {
    let rows: Vec<Vec<f64>> =
        fetch_all("SELECT array_agg(R.a) _0 FROM (SELECT 3.141592653589793::FLOAT8 a UNION ALL SELECT -2.718281828459045::FLOAT8) R")
            .await
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert!((rows[0][0] - 3.141592653589793).abs() < f64::EPSILON);
    assert!((rows[0][1] - (-2.718281828459045)).abs() < f64::EPSILON);
}

#[tokio::test]
async fn pg_arr_of_uuid_as_vec_uuid() {
    let uuid1 = "550e8400-e29b-41d4-a716-446655440000";
    let uuid2 = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
    
    let query = format!(
        "SELECT array_agg(R.a) _0 FROM (SELECT '{}'::UUID a UNION ALL SELECT '{}'::UUID) R",
        uuid1, uuid2
    );
    
    let rows: Vec<Vec<Uuid>> = fetch_all(&query).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2);
    
    let expected_uuid1 = Uuid::parse_str(uuid1).unwrap();
    let expected_uuid2 = Uuid::parse_str(uuid2).unwrap();
    
    assert!(rows[0].contains(&expected_uuid1));
    assert!(rows[0].contains(&expected_uuid2));
}

#[tokio::test]
async fn pg_arr_of_nullable_int8() {
    let rows: Vec<Vec<Option<i64>>> =
        fetch_all("SELECT array_agg(R.a) _0 FROM (SELECT 42::INT8 a UNION ALL SELECT NULL) R")
            .await
            .unwrap();
    assert_eq!(rows, vec![vec![Some(42), None]]);
}

#[tokio::test]
async fn pg_arr_of_nullable_float8() {
    let rows: Vec<Vec<Option<f64>>> =
        fetch_all("SELECT array_agg(R.a) _0 FROM (SELECT 3.14::FLOAT8 a UNION ALL SELECT NULL) R")
            .await
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2);
    assert!(rows[0][0].is_some());
    assert!((rows[0][0].unwrap() - 3.14).abs() < f64::EPSILON);
    assert!(rows[0][1].is_none());
}