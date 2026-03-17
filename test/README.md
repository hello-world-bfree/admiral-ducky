# Admiral Ducky Test Suite

This directory contains the test infrastructure for the Admiral Ducky DuckDB extension.

## Running Tests

Tests use the DuckDB Python client and SQLLogicTest format.

### Prerequisites

Ensure dependencies are configured from the project root:

```shell
make configure
```

### Running Tests

From the project root:

```shell
# Test debug build
make test_debug

# Test release build
make test_release
```

### Test Files

Test cases are in `test/sql/admiral_ducky.test` using SQLLogicTest format.

### Version Switching

Test with different DuckDB versions:

```shell
make clean_all
DUCKDB_TEST_VERSION=v1.3.2 make configure
make debug
make test_debug
```

## Example Usage

```sql
LOAD './build/debug/extension/admiral_ducky/admiral_ducky.duckdb_extension';

-- S3 operations
SELECT s3_transform('s3://bucket/file.txt');

-- HTTP requests
SELECT http_post('https://api.example.com', '{}');

-- Rhai transformations
SELECT rhai('hello', 'text.to_upper()');
```
