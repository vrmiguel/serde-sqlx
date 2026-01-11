use crate::fetch_all;
use serde_json::Value as JsValue;

#[tokio::test]
async fn pg_arr_of_bool_as_vec_bool() {
    let rows: Vec<Vec<bool>> =
        fetch_all("SELECT array_agg(R.a) _0 FROM (SELECT true a UNION ALL SELECT false) R")
            .await
            .unwrap();
    assert_eq!(rows, vec![vec![true, false]]);
}

#[tokio::test]
async fn pg_arr_of_int4_as_vec_i32() {
    let rows: Vec<Vec<i32>> =
        fetch_all("SELECT array_agg(R.a) _0 FROM (SELECT 42 a UNION ALL SELECT 0) R")
            .await
            .unwrap();
    assert_eq!(rows, vec![vec![42, 0]]);
}

#[tokio::test]
async fn pg_arr_of_text() {
    let rows: Vec<Vec<String>> = fetch_all(
        "SELECT array_agg(R.a) _0 FROM (SELECT 'a string' as a UNION ALL SELECT 'another string' as a) R"
    )
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![vec!["a string".to_owned(), "another string".to_owned()]]
    );
}

#[tokio::test]
async fn pg_arr_of_text_as_vec_nullable_string() {
    let rows: Vec<Vec<Option<String>>> =
        fetch_all("SELECT array_agg(R.a) _0 FROM (SELECT 'a string' a UNION ALL SELECT NULL) R")
            .await
            .unwrap();

    assert_eq!(rows, vec![vec![Some("a string".to_owned()), None]]);
}

#[tokio::test]
async fn pg_arr_of_varchar_as_vec_nullable_string() {
    let rows: Vec<Vec<Option<String>>> = fetch_all(
        "SELECT array_agg(R.a) _0 FROM (SELECT 'a string'::VARCHAR a UNION ALL SELECT NULL) R",
    )
    .await
    .unwrap();
    assert_eq!(rows, vec![vec![Some("a string".to_owned()), None]]);
}

#[tokio::test]
async fn pg_arr_of_jsonb_as_vec_of_jsvalue() {
    let rows: Vec<Vec<JsValue>> = fetch_all(
        "SELECT array_agg(R.a) _0 FROM (SELECT '1'::JSONB a UNION ALL SELECT '2'::JSONB) R",
    )
    .await
    .unwrap();
    assert_eq!(rows, vec![vec![serde_json::json!(1), serde_json::json!(2)]]);
}
