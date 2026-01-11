use crate::fetch_all;
use std::collections::HashMap;

#[tokio::test]
async fn text_columns_into_hashmap() {
    let rows: Vec<HashMap<String, String>> =
        fetch_all("SELECT '1' AS one, '2' AS two, '3' AS three")
            .await
            .unwrap();
    let expected_hashmap: HashMap<String, String> = vec![
        ("one".to_owned(), "1".to_owned()),
        ("two".to_owned(), "2".to_owned()),
        ("three".to_owned(), "3".to_owned()),
    ]
    .into_iter()
    .collect();
    assert_eq!(rows, vec![expected_hashmap]);
}

#[tokio::test]
async fn int4_columns_into_hashmap() {
    let rows: Vec<HashMap<String, i32>> = fetch_all("SELECT 1 AS one, 2 AS two, 3 AS three")
        .await
        .unwrap();
    let expected_hashmap: HashMap<String, i32> = vec![
        ("one".to_owned(), 1),
        ("two".to_owned(), 2),
        ("three".to_owned(), 3),
    ]
    .into_iter()
    .collect();
    assert_eq!(rows, vec![expected_hashmap]);
}
