use crate::{fetch_one, fetch_optional};
use paste::paste;

macro_rules! create_int_tests {
    (signed $ty: ident) => (
        paste! {
            #[tokio::test]
            async fn [<unannotated_as_ $ty>]() {
                let row: i64 = fetch_one("SELECT 42 AS value").await.unwrap();
                assert_eq!(row, 42);
            }

            #[tokio::test]
            async fn [<cast_as_ $ty>]() {
                let row: i64 = fetch_one("SELECT CAST(42 AS SIGNED) AS value")
                    .await
                    .unwrap();
                assert_eq!(row, 42);
            }

            #[tokio::test]
            async fn [<unannotated_as_ $ty _opt>]() {
                let row: Option<i64> = fetch_optional("SELECT 42 AS value UNION ALL SELECT NULL")
                    .await
                    .unwrap();
                assert_eq!(row, Some(42));
            }

            #[tokio::test]
            async fn [<cast_as_ $ty _opt>]() {
                let row: Option<i64> = fetch_optional("SELECT CAST(42 AS SIGNED) AS value UNION ALL SELECT NULL")
                    .await
                    .unwrap();
                assert_eq!(row, Some(42));
            }

        }
    );
    (unsigned $ty: ident) => (
        paste! {
            #[tokio::test]
            async fn [<unannotated_as_ $ty>]() {
                let row: $ty = fetch_one("SELECT 42 AS value").await.unwrap();
                assert_eq!(row, 42);
            }

            #[tokio::test]
            async fn [<cast_as_ $ty>]() {
                let row: $ty = fetch_one("SELECT CAST(42 AS UNSIGNED) AS value")
                    .await
                    .unwrap();
                assert_eq!(row, 42);
            }

            #[tokio::test]
            async fn [<unannotated_as_ $ty _opt>]() {
                let row: Option<$ty> = fetch_optional("SELECT 42 AS value UNION ALL SELECT NULL")
                    .await
                    .unwrap();
                assert_eq!(row, Some(42));
            }

            #[tokio::test]
            async fn [<cast_as_ $ty _opt>]() {
                let row: Option<$ty> = fetch_optional("SELECT CAST(42 AS UNSIGNED) AS value UNION ALL SELECT NULL")
                    .await
                    .unwrap();
                assert_eq!(row, Some(42));
            }

        }
    )
}

create_int_tests!(signed   i8);
create_int_tests!(unsigned u8);
create_int_tests!(signed   i16);
create_int_tests!(unsigned u16);
create_int_tests!(signed   i32);
create_int_tests!(unsigned u32);
create_int_tests!(signed   i64);
create_int_tests!(unsigned u64);
