mod util;

use util::fetch_all;

#[tokio::test]
async fn tuple_struct() {
    #[derive(Debug, serde::Deserialize, PartialEq, Eq)]
    struct TupleStruct(bool, i32);

    let rows: Vec<TupleStruct> = fetch_all("SELECT true _0, 42 _1 UNION ALL SELECT false, 0")
        .await
        .unwrap();
    assert_eq!(rows, vec![TupleStruct(true, 42), TupleStruct(false, 0)]);
}

#[tokio::test]
async fn tuple() {
    let rows: Vec<(bool, i32)> = fetch_all("SELECT true _0, 42 _1 UNION ALL SELECT false, 0")
        .await
        .unwrap();
    assert_eq!(rows, vec![(true, 42), (false, 0)]);
}

#[tokio::test]
async fn a_one_item_tuple() {
    let rows: Vec<(bool,)> = fetch_all("SELECT true _0 UNION ALL SELECT false")
        .await
        .unwrap();
    assert_eq!(rows, vec![(true,), (false,)]);
}

#[tokio::test]
#[ignore]
async fn a_one_item_tuple_unannotated() {
    let rows: Vec<(bool,)> = fetch_all("SELECT true UNION ALL SELECT false")
        .await
        .unwrap();
    assert_eq!(rows, vec![(true,), (false,)]);
}
