extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use duckdb::Connection;
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use std::error::Error;

mod state;
mod s3;
mod http;
mod rhai;
mod embed;
mod extraction;
mod lang;

use state::ExtensionState;
use s3::{S3Exists, S3Fetch, S3Put, S3Transform, S3TransformScript, S3TransformWith};
use http::{HttpPost, HttpPostBatchVTab, HttpPostRhai};
use rhai::RhaiTransform;
use embed::{ChunkText, Embed, TokenCount};
use extraction::HtmlExtractVTab;
use lang::IsEnglish;

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    let state = ExtensionState::new();

    con.register_scalar_function_with_state::<S3Fetch>("s3_fetch", &state)
        .expect("Failed to register s3_fetch scalar function");

    con.register_scalar_function_with_state::<S3Put>("s3_put", &state)
        .expect("Failed to register s3_put scalar function");

    con.register_scalar_function_with_state::<S3Exists>("s3_exists", &state)
        .expect("Failed to register s3_exists scalar function");

    con.register_scalar_function_with_state::<RhaiTransform>("rhai", &state)
        .expect("Failed to register rhai scalar function");

    con.register_scalar_function_with_state::<Embed>("embed", &state)
        .expect("Failed to register embed scalar function");

    con.register_scalar_function_with_state::<S3Transform>("s3_transform", &state)
        .expect("Failed to register s3_transform scalar function");

    con.register_scalar_function_with_state::<S3TransformWith>("s3_transform_with", &state)
        .expect("Failed to register s3_transform_with scalar function");

    con.register_scalar_function_with_state::<S3TransformScript>("s3_transform_script", &state)
        .expect("Failed to register s3_transform_script scalar function");

    con.register_scalar_function_with_state::<HttpPost>("http_post", &state)
        .expect("Failed to register http_post scalar function");

    con.register_scalar_function_with_state::<HttpPostRhai>("http_post_rhai", &state)
        .expect("Failed to register http_post_rhai scalar function");

    con.register_table_function::<HttpPostBatchVTab>("http_post_batch")
        .expect("Failed to register http_post_batch table function");

    con.register_table_function::<HtmlExtractVTab>("html_extract")
        .expect("Failed to register html_extract table function");

    con.register_scalar_function_with_state::<IsEnglish>("is_english", &state)
        .expect("Failed to register is_english scalar function");

    con.register_scalar_function_with_state::<TokenCount>("token_count", &state)
        .expect("Failed to register token_count scalar function");

    con.register_scalar_function_with_state::<ChunkText>("chunk_text", &state)
        .expect("Failed to register chunk_text scalar function");

    Ok(())
}
