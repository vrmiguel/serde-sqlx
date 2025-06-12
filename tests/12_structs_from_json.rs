mod util;

use serde::Deserialize;
use util::{fetch_all, fetch_one};

use serde_json::Value as JsValue;

#[tokio::test]
async fn single_json_field_into_a_record() {
    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct Record {
        one: i32,
        two: i32,
        three: i32,
    }

    let row: Record = util::fetch_one(
        r#"
            SELECT '{"one": 1, "two": 2, "three": 3}' :: JSON
        "#,
    )
    .await
    .unwrap();

    assert_eq!(
        row,
        Record {
            one: 1,
            two: 2,
            three: 3,
        }
    );
}

#[tokio::test]
async fn a_record_with_vec_of_js_value_fields() {
    #[derive(Debug, serde::Deserialize, PartialEq, Eq)]
    struct Record {
        integers: Vec<i32>,
        jsons: Vec<JsValue>,
        jsonbs: Vec<JsValue>,
    }

    let rows: Vec<Record> = util::fetch_all(
        r#"
            SELECT array_agg(R.i) integers, array_agg(R.j) jsons, array_agg(R.b) jsonbs FROM (
                SELECT 1 i, '1' :: JSON j, '1' :: JSONB b 
                UNION ALL
                SELECT 2, '2' :: JSON, '2' :: JSONB 
            ) R
        "#,
    )
    .await
    .unwrap();

    assert_eq!(
        rows,
        vec![Record {
            integers: vec![1, 2],
            jsons: vec![serde_json::json!(1), serde_json::json!(2)],
            jsonbs: vec![serde_json::json!(1), serde_json::json!(2)]
        }]
    );
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

#[tokio::test]
async fn empty_json_and_jsonb_arrays() {
    #[derive(Debug, serde::Deserialize, PartialEq, Eq)]
    struct Record {
        empty_jsons: Vec<JsValue>,
        empty_jsonbs: Vec<JsValue>,
    }

    let row: Record = fetch_one(
        r#"
            SELECT 
                '[]'::JSON[] AS empty_jsons, 
                '[]'::JSONB[] AS empty_jsonbs
        "#,
    )
    .await
    .unwrap();

    assert_eq!(
        row,
        Record {
            empty_jsons: vec![],
            empty_jsonbs: vec![],
        }
    );
}

#[tokio::test]
async fn nullable_json_and_jsonb_array_elements() {
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct Record {
        nullable_jsons: Vec<Option<JsValue>>,
        nullable_jsonbs: Vec<Option<JsValue>>,
    }

    let row: Record = fetch_one(
        r#"
            SELECT 
                ARRAY[NULL, '42'::JSON, NULL, '{"key":"value"}'::JSON]::JSON[] AS nullable_jsons,
                ARRAY[NULL, '42'::JSONB, NULL, '{"key":"value"}'::JSONB]::JSONB[] AS nullable_jsonbs
        "#,
    )
    .await
    .unwrap();

    assert_eq!(
        row,
        Record {
            nullable_jsons: vec![
                None,
                Some(serde_json::json!(42)),
                None,
                Some(serde_json::json!({"key":"value"})),
            ],
            nullable_jsonbs: vec![
                None,
                Some(serde_json::json!(42)),
                None,
                Some(serde_json::json!({"key":"value"})),
            ],
        }
    );
}

#[tokio::test]
async fn json_and_jsonb_arrays_with_mixed_types() {
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct Record {
        mixed_jsons: Vec<JsValue>,
        mixed_jsonbs: Vec<JsValue>,
    }

    let row: Record = fetch_one(
        r#"
            SELECT 
                ARRAY[
                    '42'::JSON, 
                    '"string"'::JSON, 
                    'true'::JSON, 
                    '3.14'::JSON, 
                    '{"key":"value"}'::JSON, 
                    '[1,2,3]'::JSON
                ]::JSON[] AS mixed_jsons,
                ARRAY[
                    '42'::JSONB, 
                    '"string"'::JSONB, 
                    'true'::JSONB, 
                    '3.14'::JSONB, 
                    '{"key":"value"}'::JSONB, 
                    '[1,2,3]'::JSONB
                ]::JSONB[] AS mixed_jsonbs
        "#,
    )
    .await
    .unwrap();

    assert_eq!(
        row,
        Record {
            mixed_jsons: vec![
                serde_json::json!(42),
                serde_json::json!("string"),
                serde_json::json!(true),
                serde_json::json!(3.14),
                serde_json::json!({"key":"value"}),
                serde_json::json!([1,2,3]),
            ],
            mixed_jsonbs: vec![
                serde_json::json!(42),
                serde_json::json!("string"),
                serde_json::json!(true),
                serde_json::json!(3.14),
                serde_json::json!({"key":"value"}),
                serde_json::json!([1,2,3]),
            ],
        }
    );
}

#[tokio::test]
async fn nested_json_and_jsonb_arrays() {
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct Record {
        nested_jsons: Vec<JsValue>,
        nested_jsonbs: Vec<JsValue>,
    }

    let row: Record = fetch_one(
        r#"
            SELECT 
                ARRAY[
                    '[1, 2, 3]'::JSON,
                    '{"array": [4, 5, 6]}'::JSON,
                    '[{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}]'::JSON
                ]::JSON[] AS nested_jsons,
                ARRAY[
                    '[1, 2, 3]'::JSONB,
                    '{"array": [4, 5, 6]}'::JSONB,
                    '[{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}]'::JSONB
                ]::JSONB[] AS nested_jsonbs
        "#,
    )
    .await
    .unwrap();

    assert_eq!(
        row,
        Record {
            nested_jsons: vec![
                serde_json::json!([1, 2, 3]),
                serde_json::json!({"array": [4, 5, 6]}),
                serde_json::json!([{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}]),
            ],
            nested_jsonbs: vec![
                serde_json::json!([1, 2, 3]),
                serde_json::json!({"array": [4, 5, 6]}),
                serde_json::json!([{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}]),
            ],
        }
    );
}

#[tokio::test]
async fn json_array_with_large_dataset() {
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct Record {
        large_json_array: Vec<JsValue>,
    }

    let row: Record = fetch_one(
        r#"
            SELECT 
                ARRAY(
                    SELECT json_build_object(
                        'id', generate_series(1, 100),
                        'name', concat('item', generate_series(1, 100)),
                        'value', generate_series(1, 100) * 10
                    )
                )::JSON[] AS large_json_array
        "#,
    )
    .await
    .unwrap();

    assert_eq!(row.large_json_array.len(), 100);
    assert_eq!(row.large_json_array[0], serde_json::json!({"id": 1, "name": "item1", "value": 10}));
    assert_eq!(row.large_json_array[99], serde_json::json!({"id": 100, "name": "item100", "value": 1000}));
}

#[tokio::test]
async fn deeply_nested_json_structure() {
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct Record {
        deep_json: Vec<JsValue>,
    }

    let row: Record = fetch_one(
        r#"
            SELECT 
                ARRAY[
                    '{
                        "level1": {
                            "level2": {
                                "level3": {
                                    "level4": {
                                        "data": [1, 2, 3],
                                        "info": {"type": "nested", "depth": 4}
                                    }
                                }
                            }
                        }
                    }'::JSON
                ]::JSON[] AS deep_json
        "#,
    )
    .await
    .unwrap();

    let expected = serde_json::json!({
        "level1": {
            "level2": {
                "level3": {
                    "level4": {
                        "data": [1, 2, 3],
                        "info": {"type": "nested", "depth": 4}
                    }
                }
            }
        }
    });

    assert_eq!(row.deep_json[0], expected);
}

#[tokio::test]
async fn typed_json_struct_array() {
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct Item {
        id: i32,
        name: String,
    }

    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct Record {
        typed_items: Vec<Item>,
    }

    let row: Record = fetch_one(
        r#"
            SELECT 
                ARRAY[
                    '{"id": 1, "name": "item1"}'::JSON,
                    '{"id": 2, "name": "item2"}'::JSON,
                    '{"id": 3, "name": "item3"}'::JSON
                ]::JSON[] AS typed_items
        "#,
    )
    .await
    .unwrap();

    assert_eq!(
        row,
        Record {
            typed_items: vec![
                Item { id: 1, name: "item1".to_string() },
                Item { id: 2, name: "item2".to_string() },
                Item { id: 3, name: "item3".to_string() },
            ],
        }
    );
}