use crate::{fetch_all, fetch_one, fetch_optional};

#[tokio::test]
async fn unannotated_as_string() {
    let row: String = fetch_one("SELECT 'a string' AS greeting").await.unwrap();
    assert_eq!(row, "a string");
}

#[tokio::test]
async fn char_as_string() {
    let row: String = fetch_one("SELECT CAST('a string' AS CHAR) AS greeting")
        .await
        .unwrap();
    assert_eq!(row, "a string");
}

#[tokio::test]
async fn vec_unannotated_as_string_opt() {
    let rows: Vec<Option<String>> =
        fetch_all("SELECT 'a string' AS greeting UNION ALL SELECT NULL")
            .await
            .unwrap();
    assert_eq!(rows, vec![Some("a string".to_owned()), None]);
}

#[tokio::test]
async fn vec_char_as_string_opt() {
    let rows: Vec<Option<String>> =
        fetch_all("SELECT CAST('a string' AS CHAR) AS greeting UNION ALL SELECT NULL")
            .await
            .unwrap();
    assert_eq!(rows, vec![Some("a string".to_owned()), None]);
}

#[tokio::test]
async fn char_as_string_opt() {
    let rows: Option<String> =
        fetch_optional("SELECT CAST('a string' AS CHAR) AS greeting UNION ALL SELECT NULL")
            .await
            .unwrap();

    assert_eq!(rows, Some("a string".to_owned()));
}
