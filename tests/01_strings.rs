mod util;

use util::fetch_one;

#[tokio::test]
async fn select_strings() {
    let row: String = fetch_one("SELECT 'hello' AS greeting").await.unwrap();

    assert_eq!(row, "hello");
}
