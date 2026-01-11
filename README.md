# serde-sqlx

Allows deserializing rows into Rust types using `serde`. Work in progress.

## Implementation Status

| Feature | Postgres | MySQL | SQLite |
| -------------- | --------------- | ------ | ----- |
| Primitives | ✅ | ✅ | ❌ |
| Structs | ✅ | ✅ | ❌ |
| Tuples | ✅ | ✅ | ❌ |
| Arrays | ✅ | - | - |
| JSON | ✅ | ✅ | - |
| UUID | ❌ | ❌ | ❌ |
| Enums | ❌ | ✅ | ❌ |
| chrono Date objects | ❌ | ✅ | ❌ |


> [!NOTE]
> ❌ = Planned or untested
> - = Not applicable

### Features

- **Simple Primitives**:
  - Strings: TEXT, VARCHAR, BPCHAR
  - Booleans
  - Integers: i16, i32, i64 (INT2, INT4, INT8/BIGINT)
  - Floating point: f32 (REAL), f64 (DOUBLE PRECISION)
  - Support for special float values (NaN, Infinity)

- **Structs and Tuples**:
  - Deserialize into named structs with primitive fields
  - Support for tuple structs and anonymous tuples
  - Deep nesting of structs using Serde's flattening

- **Optional Values**:
  - NULL values into Option<T>

- **JSON and JSONB**:
  - Directly deserialize JSON data into Rust structures

- **Newtypes**:
  - Support for newtype pattern (e.g., `struct UserId(i32)`)

- **PostgreSQL Arrays**:
  - Convert Postgres arrays into Rust vectors
  - Support for arrays of primitive types and nullable types

## Usage

Add `serde-sqlx` to your Cargo.toml:

```toml
[dependencies]
serde-sqlx = "1.0.0"
serde = { version = "1", features = ["derive"] }
sqlx = { version = "0.6", features = ["postgres", "runtime-tokio-native-tls"] }
```

Basic example:

```rust
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct User {
    id: i32,
    name: String,
    active: bool,
    profile: Option<Profile>,
}

#[derive(Deserialize, Debug)]
struct Profile {
    bio: String,
    age: i32,
}

async fn get_users(pool: &PgPool) -> anyhow::Result<Vec<User>> {
    let rows = sqlx::query(
        "SELECT id, name, active, profile::JSONB FROM users"
    ).fetch_all(pool).await?;

    let users: Result<Vec<_>, _> = rows.into_iter()
        .map(serde_sqlx::from_row)
        .collect();

    users.map_err(Into::into)
}
```
