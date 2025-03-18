mod util;

use serde::Deserialize;
use util::fetch_all;

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
