use crate::{fetch_all, fetch_one};
use serde::Deserialize;

use serde_json::Value as JsValue;

#[tokio::test]
async fn single_json_field_into_a_record() {
    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct Record {
        one: i32,
        two: i32,
        three: i32,
    }

    let row: Record = fetch_one(
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
#[ignore = "TODO: doesn't work yet"]
async fn a_record_with_vec_of_js_value_fields() {
    #[derive(Debug, serde::Deserialize, PartialEq, Eq)]
    struct Record {
        integers: Vec<i32>,
        jsons: Vec<JsValue>,
        jsonbs: Vec<JsValue>,
    }

    let rows: Vec<Record> = fetch_all(
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
