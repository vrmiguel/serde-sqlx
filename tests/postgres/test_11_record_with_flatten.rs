use crate::fetch_all;
use std::collections::HashMap;

#[tokio::test]
async fn text_columns_into_hashmap() {
    #[derive(Debug, ::serde::Deserialize, PartialEq, Eq)]
    struct Record {
        #[serde(flatten)]
        fields: HashMap<String, i32>,
    }

    let rows: Vec<Record> = fetch_all("SELECT 1 one, 2 two, 3 three").await.unwrap();
    let fields: HashMap<String, i32> = vec![
        ("one".to_owned(), 1),
        ("two".to_owned(), 2),
        ("three".to_owned(), 3),
    ]
    .into_iter()
    .collect();
    assert_eq!(rows, vec![Record { fields }]);
}

#[tokio::test]
async fn int4_columns_into_struct_with_flattenned_field() {
    #[derive(Debug, ::serde::Deserialize, PartialEq, Eq)]
    struct Record {
        one: i32,
        #[serde(flatten)]
        the_rest: HashMap<String, i32>,
    }
    let rows: Vec<Record> = fetch_all("SELECT 1 one, 2 two, 3 three").await.unwrap();
    let the_rest: HashMap<String, i32> = vec![("two".to_owned(), 2), ("three".to_owned(), 3)]
        .into_iter()
        .collect();
    assert_eq!(rows, vec![Record { one: 1, the_rest }]);
}

#[tokio::test]
#[ignore]
async fn int4_columns_into_struct_with_flattened_field_with_tuple_idx_prefix() {
    #[derive(Debug, ::serde::Deserialize, PartialEq, Eq)]
    struct Record {
        one: i32,
        #[serde(flatten)]
        the_rest: HashMap<String, i32>,
    }
    let rows: Vec<(Record, i32)> = fetch_all("SELECT 1 _0_one, 2 _0_two, 3 _0_three, 4 _1")
        .await
        .unwrap();
    let the_rest: HashMap<String, i32> = vec![("two".to_owned(), 2), ("three".to_owned(), 3)]
        .into_iter()
        .collect();
    assert_eq!(rows, vec![(Record { one: 1, the_rest }, 4)]);
}
