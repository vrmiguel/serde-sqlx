use crate::fetch_all;

#[tokio::test]
async fn two_struct_fields_struct() {
    #[derive(Debug, ::serde::Deserialize, PartialEq, Eq)]
    struct Left {
        first_field: bool,
    }
    #[derive(Debug, ::serde::Deserialize, PartialEq, Eq)]
    struct Right {
        second_field: i32,
    }
    #[derive(Debug, ::serde::Deserialize, PartialEq, Eq)]
    struct TwoStructFieldsStruct {
        #[serde(flatten)]
        left: Left,
        #[serde(flatten)]
        right: Right,
    }

    let rows: Vec<TwoStructFieldsStruct> =
        fetch_all("SELECT true AS first_field, 42 AS second_field UNION ALL SELECT false, 0")
            .await
            .unwrap();
    assert_eq!(
        rows,
        vec![
            TwoStructFieldsStruct {
                left: Left { first_field: true },
                right: Right { second_field: 42 },
            },
            TwoStructFieldsStruct {
                left: Left { first_field: false },
                right: Right { second_field: 0 },
            },
        ]
    );
}

#[tokio::test]
async fn deep_struct() {
    #[derive(Debug, ::serde::Deserialize, PartialEq, Eq)]
    struct A {
        the_field: bool,
    }
    #[derive(Debug, ::serde::Deserialize, PartialEq, Eq)]
    struct B {
        #[serde(flatten)]
        a: A,
    }
    #[derive(Debug, ::serde::Deserialize, PartialEq, Eq)]
    struct DeepStruct {
        #[serde(flatten)]
        b: B,
    }

    let rows: Vec<DeepStruct> = fetch_all("SELECT true AS the_field UNION ALL SELECT false")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            DeepStruct {
                b: B {
                    a: A { the_field: true }
                },
            },
            DeepStruct {
                b: B {
                    a: A { the_field: false }
                },
            },
        ]
    );
}
