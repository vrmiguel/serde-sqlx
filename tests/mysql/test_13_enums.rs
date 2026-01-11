use serde::Deserialize;

use crate::fetch_one;

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Color {
    Red,
    Green,
    Purple,
    Magenta,
    Cyan,
    Orange,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Data {
    color: Color,
}

#[tokio::test]
async fn test_enum_by_itself() {
    let color: Color = fetch_one(" SELECT 'red' ").await.unwrap();

    assert_eq!(color, Color::Red);
}

#[tokio::test]
async fn test_enum_inside_struct() {
    let data: Data = fetch_one(" SELECT 'magenta' AS `color`").await.unwrap();

    assert_eq!(
        data,
        Data {
            color: Color::Magenta
        }
    );
}
