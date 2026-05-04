use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::{
    error::Error,
    sync::atomic::{AtomicUsize, Ordering},
};

struct CatalogEntry {
    name: &'static str,
    kind: &'static str,
    module: &'static str,
    category: &'static str,
    signature: &'static str,
    description: &'static str,
    example: &'static str,
}

const CATALOG: &[CatalogEntry] = &[
    CatalogEntry {
        name: "s3_fetch",
        kind: "scalar",
        module: "s3",
        category: "S3",
        signature: "s3_fetch(path VARCHAR) -> VARCHAR",
        description: "Fetch the body of an S3 object as text. Requires AWS credentials in environment.",
        example: "SELECT s3_fetch('s3://example-bucket/key.txt'); -- requires AWS credentials",
    },
    CatalogEntry {
        name: "s3_put",
        kind: "scalar",
        module: "s3",
        category: "S3",
        signature: "s3_put(path VARCHAR, content VARCHAR) -> VARCHAR",
        description: "Write a string to S3, return the path. Requires AWS credentials.",
        example: "SELECT s3_put('s3://example-bucket/out.txt', 'hello'); -- requires AWS credentials",
    },
    CatalogEntry {
        name: "s3_exists",
        kind: "scalar",
        module: "s3",
        category: "S3",
        signature: "s3_exists(path VARCHAR) -> BOOLEAN",
        description: "Check whether an S3 object exists. Requires AWS credentials.",
        example: "SELECT s3_exists('s3://example-bucket/key'); -- requires AWS credentials",
    },
    CatalogEntry {
        name: "s3_transform",
        kind: "scalar",
        module: "s3",
        category: "S3",
        signature: "s3_transform(path VARCHAR) -> VARCHAR",
        description: "Fetch and apply default newline-stripping transform. Requires AWS credentials.",
        example: "SELECT s3_transform('s3://example-bucket/key.txt'); -- requires AWS credentials",
    },
    CatalogEntry {
        name: "s3_transform_with",
        kind: "scalar",
        module: "s3",
        category: "S3",
        signature: "s3_transform_with(path VARCHAR, mode VARCHAR) -> VARCHAR",
        description: "Fetch S3 object and apply named transform (trim, upper, lower, etc.). Requires AWS credentials.",
        example: "SELECT s3_transform_with('s3://example-bucket/key.txt', 'trim'); -- requires AWS credentials",
    },
    CatalogEntry {
        name: "s3_transform_script",
        kind: "scalar",
        module: "s3",
        category: "S3",
        signature: "s3_transform_script(path VARCHAR, rhai_script VARCHAR) -> VARCHAR",
        description: "Fetch S3 object and transform via Rhai script (input bound as text). Requires AWS credentials.",
        example: "SELECT s3_transform_script('s3://example-bucket/key', 'text.to_upper()'); -- requires AWS credentials",
    },
    CatalogEntry {
        name: "http_post",
        kind: "scalar",
        module: "http",
        category: "HTTP",
        signature: "http_post(url VARCHAR, body VARCHAR [, headers_json VARCHAR]) -> STRUCT(status_code INT, body VARCHAR, headers VARCHAR)",
        description: "POST body to URL with retry on 429/5xx. Requires network.",
        example: "SELECT http_post('https://httpbin.org/post', '{\"k\":1}'); -- requires network",
    },
    CatalogEntry {
        name: "http_post_rhai",
        kind: "scalar",
        module: "http",
        category: "HTTP",
        signature: "http_post_rhai(url VARCHAR, text VARCHAR, rhai_script VARCHAR [, headers_json VARCHAR]) -> STRUCT(...)",
        description: "Apply Rhai transform to text then POST. Requires network.",
        example: "SELECT http_post_rhai('https://httpbin.org/post', 'hi', 'text.to_upper()'); -- requires network",
    },
    CatalogEntry {
        name: "http_post_batch",
        kind: "table",
        module: "http",
        category: "HTTP",
        signature: "http_post_batch(url VARCHAR, payloads_json VARCHAR, batch_size VARCHAR, headers_json VARCHAR) -> TABLE(batch_id INT, row_count INT, status_code INT, response_body VARCHAR)",
        description: "Concurrently POST a JSON array of payloads in batches of N. Requires network.",
        example: "SELECT * FROM http_post_batch('https://httpbin.org/post', '[\"a\",\"b\",\"c\"]', '2', ''); -- requires network",
    },
    CatalogEntry {
        name: "rhai",
        kind: "scalar",
        module: "rhai",
        category: "Scripting",
        signature: "rhai(text VARCHAR, script VARCHAR) -> VARCHAR",
        description: "Run Rhai script with `text` bound to input.",
        example: "SELECT rhai('hello', 'text.to_upper()');",
    },
    CatalogEntry {
        name: "embed",
        kind: "scalar",
        module: "embed",
        category: "AI",
        signature: "embed(text VARCHAR [, endpoint VARCHAR]) -> DOUBLE[]",
        description: "Generate embedding via gRPC endpoint. Requires gRPC embedding server (default localhost:50051).",
        example: "SELECT embed('hello world', 'localhost:50051'); -- requires gRPC server at endpoint",
    },
    CatalogEntry {
        name: "embed_fake",
        kind: "scalar",
        module: "embed",
        category: "AI",
        signature: "embed_fake(text VARCHAR, dim INTEGER) -> DOUBLE[]",
        description: "Deterministic fake embedding for testing — no endpoint needed.",
        example: "SELECT embed_fake('hello', 768);",
    },
    CatalogEntry {
        name: "token_count",
        kind: "scalar",
        module: "embed",
        category: "AI",
        signature: "token_count(text VARCHAR [, endpoint VARCHAR]) -> UINTEGER",
        description: "Count tokens via gRPC endpoint. Requires gRPC embedding server.",
        example: "SELECT token_count('hello world', 'localhost:50051'); -- requires gRPC server at endpoint",
    },
    CatalogEntry {
        name: "chunk_text",
        kind: "scalar",
        module: "embed",
        category: "AI",
        signature: "chunk_text(text VARCHAR, max_tokens INTEGER [, endpoint VARCHAR]) -> VARCHAR[]",
        description: "Chunk text by token budget via gRPC endpoint. Requires gRPC embedding server.",
        example: "SELECT chunk_text('long text', 512, 'localhost:50051'); -- requires gRPC server at endpoint",
    },
    CatalogEntry {
        name: "is_english",
        kind: "scalar",
        module: "lang",
        category: "Text",
        signature: "is_english(text VARCHAR) -> BOOLEAN",
        description: "Detect English using whatlang heuristics.",
        example: "SELECT is_english('hello world');",
    },
    CatalogEntry {
        name: "html_extract",
        kind: "table",
        module: "extraction",
        category: "Text",
        signature: "html_extract(html VARCHAR [, config VARCHAR]) -> TABLE(...)",
        description: "Extract & chunk HTML; pass mode (semantic/paragraph/raw) or JSON config.",
        example: "SELECT * FROM html_extract('<h1>T</h1><p>body</p>', 'paragraph');",
    },
];

pub struct AdmiralDuckyFunctionsBindData {}

#[repr(C)]
pub struct AdmiralDuckyFunctionsInitData {
    current_row: AtomicUsize,
}

pub struct AdmiralDuckyFunctionsVTab;

impl VTab for AdmiralDuckyFunctionsVTab {
    type InitData = AdmiralDuckyFunctionsInitData;
    type BindData = AdmiralDuckyFunctionsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        for col in [
            "name",
            "kind",
            "module",
            "category",
            "signature",
            "description",
            "example",
        ] {
            bind.add_result_column(col, LogicalTypeHandle::from(LogicalTypeId::Varchar));
        }
        Ok(AdmiralDuckyFunctionsBindData {})
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(AdmiralDuckyFunctionsInitData {
            current_row: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init = func.get_init_data();
        let current = init.current_row.load(Ordering::Relaxed);

        if current >= CATALOG.len() {
            output.set_len(0);
            return Ok(());
        }

        let remaining = CATALOG.len() - current;
        let chunk_size = remaining.min(2048);

        for i in 0..chunk_size {
            let e = &CATALOG[current + i];
            output.flat_vector(0).insert(i, e.name);
            output.flat_vector(1).insert(i, e.kind);
            output.flat_vector(2).insert(i, e.module);
            output.flat_vector(3).insert(i, e.category);
            output.flat_vector(4).insert(i, e.signature);
            output.flat_vector(5).insert(i, e.description);
            output.flat_vector(6).insert(i, e.example);
        }

        init.current_row
            .store(current + chunk_size, Ordering::Relaxed);
        output.set_len(chunk_size);

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}
