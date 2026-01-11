use chrono::{NaiveDate, NaiveTime};

use crate::fetch_one;

const TEST_DATE: NaiveDate = NaiveDate::from_ymd_opt(2017, 11, 16).unwrap();
const TEST_TIME: NaiveTime = NaiveTime::from_hms_micro_opt(14, 45, 12, 12_345).unwrap();

#[tokio::test]
async fn test_chrono_date() {
    let date: chrono::NaiveDate = fetch_one(
        "
        SELECT CAST('2017-11-16' AS DATE)
    ",
    )
    .await
    .unwrap();

    assert_eq!(date, chrono::NaiveDate::from_ymd_opt(2017, 11, 16).unwrap())
}

#[tokio::test]
async fn test_chrono_time() {
    let date: chrono::NaiveTime = fetch_one(
        "
        SELECT CAST('14:45:12' AS TIME)
    ",
    )
    .await
    .unwrap();

    assert_eq!(date, chrono::NaiveTime::from_hms_opt(14, 45, 12).unwrap())
}

#[tokio::test]
async fn test_chrono_datetime() {
    let date: chrono::NaiveDateTime = fetch_one(
        "
            SELECT CAST('2017-11-16 14:45:12.012345' AS DATETIME(6))
        ",
    )
    .await
    .unwrap();

    assert_eq!(date, TEST_DATE.and_time(TEST_TIME))
}
