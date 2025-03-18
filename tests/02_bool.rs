mod util;

use util::fetch_all;

#[tokio::test]
async fn deserialize_bool() {
    let rows: Vec<bool> = fetch_all("SELECT true UNION ALL SELECT false")
        .await
        .unwrap();

    assert_eq!(rows, [true, false]);
}
