use serde_json::json;

use crate::fetch_one;

#[tokio::test]
async fn test_json_value_float() {
    let val: serde_json::Value = fetch_one(
        "
            SELECT -4.2 AS value
        ",
    )
    .await
    .unwrap();

    assert_eq!(val, json!(-4.2));
}

#[tokio::test]
async fn test_json_value_string() {
    let val: serde_json::Value = fetch_one(
        "
            SELECT 'hello world!' AS value
        ",
    )
    .await
    .unwrap();

    assert_eq!(val, json!("hello world!"));
}
