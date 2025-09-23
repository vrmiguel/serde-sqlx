use serde::Deserialize;
use serde_sqlx::from_pg_row;
use sqlx::{postgres::PgPoolOptions, Row};

#[derive(Debug, Deserialize)]
struct Greeting {
    message: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello World - serde-sqlx Example");
    println!("================================");

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:password@localhost:5432/postgres".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;

    let row = sqlx::query("SELECT 'Hello, World!' as message")
        .fetch_one(&pool)
        .await?;

    let greeting: Greeting = from_pg_row(row)?;

    println!("Structured greeting: {}", greeting.message);

    let simple_greeting: String = from_pg_row(
        sqlx::query("SELECT 'Hello from serde-sqlx!'")
            .fetch_one(&pool)
            .await?,
    )?;

    println!("Simple greeting: {}", simple_greeting);

    pool.close().await;

    Ok(())
}