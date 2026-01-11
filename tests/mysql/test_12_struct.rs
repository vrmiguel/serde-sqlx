use crate::{fetch_all, fetch_one};
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
pub struct QueryStats {
    pub query: String,
    pub calls: i64,
    pub total_exec_time: f64,
    pub mean_exec_time: f64,
    pub rows: i64,
    pub total_blocks: i64,
}

#[tokio::test]
async fn test_single_query_stats() {
    // Test that a single row can be fetched and deserialized into QueryStats.
    let stats: QueryStats = fetch_one(
        r#"
            SELECT
                'dummy query' AS query,
                10 AS calls,
                123.45 AS total_exec_time,
                12.34 AS mean_exec_time,
                100 AS `rows`,
                256 AS total_blocks
        "#,
    )
    .await
    .unwrap();

    assert_eq!(
        stats,
        QueryStats {
            query: "dummy query".to_owned(),
            calls: 10,
            total_exec_time: 123.45,
            mean_exec_time: 12.34,
            rows: 100,
            total_blocks: 256,
        }
    );
}

#[tokio::test]
async fn test_multiple_query_stats() {
    // Test that multiple rows can be fetched and deserialized into a Vec<QueryStats>.
    let rows: Vec<QueryStats> = fetch_all(
        r#"
            SELECT * FROM (
                (SELECT 'query1' AS query, 5 AS calls, 200.0 AS total_exec_time, 40.0 AS mean_exec_time, 50 AS `rows`, 300 AS total_blocks)
                UNION ALL
                SELECT 'query2', 15, 400.0, 26.67, 80, 500
            ) T
        "#,
    )
    .await
    .unwrap();

    assert_eq!(
        rows,
        vec![
            QueryStats {
                query: "query1".to_owned(),
                calls: 5,
                total_exec_time: 200.0,
                mean_exec_time: 40.0,
                rows: 50,
                total_blocks: 300,
            },
            QueryStats {
                query: "query2".to_owned(),
                calls: 15,
                total_exec_time: 400.0,
                mean_exec_time: 26.67,
                rows: 80,
                total_blocks: 500,
            },
        ]
    );
}
