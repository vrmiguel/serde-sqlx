use crate::{fetch_all, fetch_one, fetch_optional};

#[tokio::test]
async fn unannotated_as_string() {
    let row: String = fetch_one("SELECT 'a string' AS greeting").await.unwrap();
    assert_eq!(row, "a string");
}

#[tokio::test]
async fn text_as_string() {
    let row: String = fetch_one("SELECT 'a string' :: TEXT AS greeting")
        .await
        .unwrap();
    assert_eq!(row, "a string");
}

#[tokio::test]
async fn bpchar_as_string() {
    let row: String = fetch_one("SELECT 'a string' :: BPCHAR AS greeting")
        .await
        .unwrap();
    assert_eq!(row, "a string");
}

#[tokio::test]
async fn varchar_as_string() {
    let row: String = fetch_one("SELECT 'a string' :: VARCHAR AS greeting")
        .await
        .unwrap();
    assert_eq!(row, "a string");
}

#[tokio::test]
async fn unannotated_as_string_opt() {
    let rows: Vec<Option<String>> =
        fetch_all("SELECT 'a string' AS greeting UNION ALL SELECT NULL")
            .await
            .unwrap();
    assert_eq!(rows, vec![Some("a string".to_owned()), None]);
}

#[tokio::test]
async fn text_as_string_opt() {
    let rows: Vec<Option<String>> =
        fetch_all("SELECT 'a string' :: TEXT AS greeting UNION ALL SELECT NULL")
            .await
            .unwrap();
    assert_eq!(rows, vec![Some("a string".to_owned()), None]);
}

#[tokio::test]
async fn bpchar_as_string_opt() {
    let rows: Vec<Option<String>> =
        fetch_all("SELECT 'a string' :: BPCHAR AS greeting UNION ALL SELECT NULL")
            .await
            .unwrap();
    assert_eq!(rows, vec![Some("a string".to_owned()), None]);
}

#[tokio::test]
async fn varchar_as_string_opt() {
    let rows: Option<String> =
        fetch_optional("SELECT 'a string' :: VARCHAR AS greeting UNION ALL SELECT NULL")
            .await
            .unwrap();

    assert_eq!(rows, Some("a string".to_owned()));
}
