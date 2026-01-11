use crate::fetch_all;

#[tokio::test]
async fn unannotated_as_bool() {
    let rows: Vec<bool> = fetch_all("SELECT true UNION ALL SELECT false")
        .await
        .unwrap();

    assert_eq!(rows, [true, false]);
}

#[tokio::test]
async fn t_unannotated_as_bool_opt() {
    let rows =
        fetch_all::<Option<bool>>("SELECT true UNION ALL SELECT false UNION ALL SELECT NULL")
            .await
            .unwrap();

    assert_eq!(rows, [Some(true), Some(false), None])
}
