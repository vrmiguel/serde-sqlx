
POSTGRES_URL := "postgres://test:pass@localhost:5432/test"
MYSQL_URL    := "mysql://test:pass@localhost:3306/test"

# Start a disposable Postgres Docker container and runs postgres tests
start-test-postgres:
    docker kill test-postgres || true
    docker run --rm --name test-postgres \
        -e POSTGRES_USER=test \
        -e POSTGRES_PASSWORD=pass \
        -e POSTGRES_DB=test \
        -p 5432:5432 \
        -d postgres:15-alpine
    # Wait for container to be ready
    sleep 4
    -DATABASE_URL={{POSTGRES_URL}} cargo test --test postgres
    docker kill test-postgres

# Start a disposable MySql Docker container and runs mysql tests
start-test-mysql:
    docker kill test-mysql || true
    docker run --rm --name test-mysql \
        -e MYSQL_ROOT_PASSWORD=passw0rd \
        -e MYSQL_DATABASE=test \
        -e MYSQL_USER=test \
        -e MYSQL_PASSWORD=pass \
        -p 3306:3306 \
        -d mysql:8.0
    sleep 5
    -DATABASE_URL={{MYSQL_URL}} cargo test --test mysql
    docker kill test-mysql
