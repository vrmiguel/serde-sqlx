use crate::fetch_all;

#[tokio::test]
async fn one_prim_field_struct() {
    #[derive(Debug, ::serde::Deserialize, PartialEq, Eq)]
    struct OneFieldStruct {
        the_field: bool,
    }

    let rows: Vec<OneFieldStruct> = fetch_all("SELECT true the_field UNION ALL SELECT false")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            OneFieldStruct { the_field: true },
            OneFieldStruct { the_field: false }
        ]
    );
}

#[tokio::test]
async fn two_prim_field_struct() {
    #[derive(Debug, ::serde::Deserialize, PartialEq, Eq)]
    struct TwoFieldStruct {
        first_field: bool,
        second_field: i32,
    }

    let rows: Vec<TwoFieldStruct> =
        fetch_all("SELECT true first_field, 42 second_field UNION ALL SELECT false, 0")
            .await
            .unwrap();
    assert_eq!(
        rows,
        vec![
            TwoFieldStruct {
                first_field: true,
                second_field: 42
            },
            TwoFieldStruct {
                first_field: false,
                second_field: 0
            },
        ]
    );
}

#[tokio::test]
async fn two_indirect_prim_fields_struct() {
    #[derive(Debug, ::serde::Deserialize, PartialEq, Eq)]
    struct Left {
        first_field: bool,
    }
    #[derive(Debug, ::serde::Deserialize, PartialEq, Eq)]
    struct Right {
        second_field: i32,
    }

    #[derive(Debug, ::serde::Deserialize, PartialEq, Eq)]
    struct OneFieldStruct {
        #[serde(flatten)]
        left: Left,
        #[serde(flatten)]
        right: Right,
    }

    let rows: Vec<OneFieldStruct> =
        fetch_all("SELECT true first_field, 42 second_field UNION ALL SELECT false, 0")
            .await
            .unwrap();
    assert_eq!(
        rows,
        vec![
            OneFieldStruct {
                left: Left { first_field: true },
                right: Right { second_field: 42 },
            },
            OneFieldStruct {
                left: Left { first_field: false },
                right: Right { second_field: 0 },
            },
        ]
    );
}
