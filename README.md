# Admiral Ducky

A DuckDB extension for advanced data retrieval and transformation. Provides SQL functions for S3 operations, HTTP requests, vector embeddings, and text transformations via Rhai scripting.

## Features

- **S3 Operations**: Fetch and write files to S3 with configurable transformations
- **HTTP Client**: POST requests with retry logic, batching, and response parsing
- **Vector Embeddings**: Generate embeddings via gRPC service integration
- **Rhai Scripting**: Apply custom text transformations using embedded Rhai scripts
- **Connection Pooling**: Efficient connection reuse for S3, HTTP, and gRPC

## Functions

### S3 Functions

| Function | Description |
|----------|-------------|
| `s3_fetch(path)` | Fetch raw content from S3 |
| `s3_put(path, content)` | Write content to S3 |
| `s3_transform(path)` | Fetch S3 content and strip newlines |
| `s3_transform_with(path, mode)` | Fetch with transformation mode: `trim`, `lowercase`, `uppercase`, `none` |
| `s3_transform_script(path, script)` | Fetch and apply Rhai transformation script |

### HTTP Functions

| Function | Description |
|----------|-------------|
| `http_post(url, body)` | POST request with automatic retries |
| `http_post(url, body, headers)` | POST with custom headers (JSON object) |
| `http_post_rhai(url, data, script)` | POST with Rhai-transformed body |
| `http_post_batch(url, payloads, batch_size, headers)` | Batch POST processing (table function) |

### Text Transformation

| Function | Description |
|----------|-------------|
| `rhai(text, script)` | Apply Rhai script to text |
| `embed(text, endpoint)` | Generate vector embeddings via gRPC |

## Building

### Dependencies

- Rust toolchain
- Python3 with venv
- Make
- Git

### Build Commands

```shell
# Clone with submodules
git clone --recurse-submodules <repo>

# Configure Python venv and DuckDB test runner
make configure

# Build debug version
make debug

# Build release version
make release
```

The extension is written to `build/debug/extension/admiral_ducky/admiral_ducky.duckdb_extension`.

## Usage

Start DuckDB with the `-unsigned` flag to load local extensions:

```bash
duckdb -unsigned
```

Load the extension and configure AWS credentials:

```sql
LOAD './build/debug/extension/admiral_ducky/admiral_ducky.duckdb_extension';
```

### Environment Variables

Set AWS credentials before starting DuckDB:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
```

### Examples

```sql
-- Fetch and transform S3 content
SELECT s3_transform('s3://my-bucket/data.txt') AS content;

-- Apply custom Rhai transformation
SELECT s3_transform_script(
    's3://my-bucket/file.txt',
    'skip_lines(text, 1)'  -- Remove header line
) AS content;

-- HTTP POST with retry
SELECT http_post(
    'https://api.example.com/data',
    '{"key": "value"}'
).body AS response;

-- Batch HTTP requests
SELECT * FROM http_post_batch(
    'https://api.example.com/batch',
    'payloads.json',
    10,
    '{"Content-Type": "application/json"}'
);

-- Generate embeddings
SELECT embed('Hello world', 'localhost:50051') AS vector;
```

## Rhai Scripting

Transform text using Rhai scripts. The file content is available as `text`.

### Built-in Functions

| Function | Description |
|----------|-------------|
| `regex_replace(text, pattern, replacement)` | Replace regex matches |
| `regex_match(text, pattern)` | Check if pattern matches |
| `squeeze_whitespace(text)` | Collapse whitespace |
| `lines(text)` | Split into line array |
| `take_lines(text, n)` | Keep first N lines |
| `skip_lines(text, n)` | Remove first N lines |
| `truncate(text, max_len)` | Truncate to length |

### Example Scripts

```sql
-- Remove CSV header
SELECT s3_transform_script(path, 'skip_lines(text, 1)') FROM files;

-- Redact phone numbers (note: escape backslashes in Rhai)
SELECT rhai(content, 'regex_replace(text, "\\d{3}-\\d{3}-\\d{4}", "[PHONE]")');

-- Normalize whitespace
SELECT rhai(content, 'squeeze_whitespace(text).trim()');
```

## Testing

```shell
# Run tests with debug build
make test_debug

# Run tests with release build
make test_release
```

### Version Switching

Test with different DuckDB versions:

```shell
make clean_all
DUCKDB_TEST_VERSION=v1.3.2 make configure
make debug
make test_debug
```

## Architecture

- **Async Runtime**: Multi-threaded Tokio runtime for concurrent operations (up to 32 parallel requests)
- **Connection Pooling**: S3 clients and gRPC channels cached per endpoint
- **Script Caching**: Rhai AST cached for repeated script execution
- **Retry Logic**: Exponential backoff for transient HTTP errors (429, 5xx)

## Known Issues

Extensions may fail to load on Windows with Python 3.11:
```
IO Error: Extension '<name>.duckdb_extension' could not be loaded: The specified module could not be found
```
Use Python 3.12 to resolve.
