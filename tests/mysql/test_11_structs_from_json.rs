use crate::{fetch_all, fetch_one};
use serde::Deserialize;

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
            SELECT
                JSON_OBJECT(
                    'one',   1,
                    'two',   2,
                    'three', 3
                ) AS json_record
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
            SELECT
                JSON_OBJECT(
                    'one',   1,
                    'two',   2,
                    'three', 3
                ) AS json_record
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
