use anyhow::Context;
use bytes::Bytes;
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    types::DuckString,
    vtab::arrow::WritableVector,
    vscalar::{ScalarFunctionSignature, VScalar},
};
use futures::stream::{self, StreamExt};
use libduckdb_sys::duckdb_string_t;
use object_store::{path::Path as ObjectPath, ObjectStore};
use rhai::AST;
use std::{error::Error, sync::Arc};

use crate::state::{ExtensionState, MAX_CONCURRENT_REQUESTS};

pub(crate) enum TransformMode {
    StripNewlines,
    Trim,
    Lowercase,
    Uppercase,
    None,
}

impl TransformMode {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "strip_newlines" | "stripnewlines" | "oneline" => TransformMode::StripNewlines,
            "trim" => TransformMode::Trim,
            "lowercase" | "lower" => TransformMode::Lowercase,
            "uppercase" | "upper" => TransformMode::Uppercase,
            "none" | "raw" => TransformMode::None,
            _ => TransformMode::StripNewlines,
        }
    }

    pub fn apply(&self, text: String) -> String {
        match self {
            TransformMode::StripNewlines => text.replace('\n', " ").replace('\r', ""),
            TransformMode::Trim => text.trim().to_string(),
            TransformMode::Lowercase => text.to_lowercase(),
            TransformMode::Uppercase => text.to_uppercase(),
            TransformMode::None => text,
        }
    }
}

pub(crate) async fn fetch_text(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<String, Box<dyn Error>> {
    let path = ObjectPath::from(key);

    let get_result = store
        .get(&path)
        .await
        .context(format!("Failed to fetch S3 object: {}", key))?;

    let bytes: Bytes = get_result
        .bytes()
        .await
        .context("Failed to read S3 object bytes")?;

    let text = String::from_utf8(bytes.to_vec())
        .context("S3 object is not valid UTF-8")?;

    Ok(text)
}

pub(crate) async fn fetch_and_transform_text(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<String, Box<dyn Error>> {
    let text = fetch_text(store, key).await?;
    Ok(TransformMode::StripNewlines.apply(text))
}

pub(crate) async fn fetch_and_transform_text_with_mode(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    mode: TransformMode,
) -> Result<String, Box<dyn Error>> {
    let text = fetch_text(store, key).await?;
    Ok(mode.apply(text))
}

pub(crate) async fn put_text(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    content: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let path = ObjectPath::from(key);
    let bytes = Bytes::from(content.to_string());

    store
        .put(&path, bytes.into())
        .await
        .map_err(|e| format!("Failed to put S3 object '{}': {}", key, e))?;

    Ok(())
}

pub(crate) fn parse_s3_path(s3_path: &str) -> Result<(String, String), Box<dyn Error>> {
    let path = s3_path
        .strip_prefix("s3://")
        .context("S3 path must start with s3://")?;

    let parts: Vec<&str> = path.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err("Invalid S3 path format. Expected s3://bucket/key".into());
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

pub(crate) struct S3Transform;

impl VScalar for S3Transform {
    type State = ExtensionState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let size = input.len();
        let input_vector = input.flat_vector(0);
        let output_vector = output.flat_vector();


        let input_data_ptr = input_vector.as_mut_ptr::<duckdb_string_t>();

        let mut tasks: Vec<(usize, Option<(String, String)>)> = Vec::with_capacity(size);
        for row_idx in 0..size {
            if input_vector.row_is_null(row_idx as u64) {
                tasks.push((row_idx, None));
                continue;
            }

            let s3_path = DuckString::new(&mut *input_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let (bucket, key) = parse_s3_path(&s3_path)?;
            tasks.push((row_idx, Some((bucket, key))));
        }

        let results: Vec<(usize, Result<String, String>)> = state.runtime.block_on(async {
            stream::iter(tasks)
                .map(|(row_idx, path_opt)| {
                    let state = state.clone();
                    async move {
                        match path_opt {
                            None => (row_idx, Ok(String::new())),
                            Some((bucket, key)) => {
                                let client = match state.get_or_create_client(&bucket) {
                                    Ok(c) => c,
                                    Err(e) => return (row_idx, Err(e.to_string())),
                                };
                                let result = fetch_and_transform_text(&client, &key).await;
                                (row_idx, result.map_err(|e| e.to_string()))
                            }
                        }
                    }
                })
                .buffer_unordered(MAX_CONCURRENT_REQUESTS)
                .collect()
                .await
        });

        for (row_idx, result) in results {
            match result {
                Ok(text) => output_vector.insert(row_idx, text.as_str()),
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
    }

    fn volatile() -> bool {
        true
    }
}

pub(crate) struct S3TransformWith;

impl VScalar for S3TransformWith {
    type State = ExtensionState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let size = input.len();
        let path_vector = input.flat_vector(0);
        let mode_vector = input.flat_vector(1);
        let output_vector = output.flat_vector();


        let path_data_ptr = path_vector.as_mut_ptr::<duckdb_string_t>();
        let mode_data_ptr = mode_vector.as_mut_ptr::<duckdb_string_t>();

        let mut tasks: Vec<(usize, Option<(String, String, TransformMode)>)> = Vec::with_capacity(size);
        for row_idx in 0..size {
            if path_vector.row_is_null(row_idx as u64) {
                tasks.push((row_idx, None));
                continue;
            }

            let s3_path = DuckString::new(&mut *path_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let mode_str = if mode_vector.row_is_null(row_idx as u64) {
                "strip_newlines".to_string()
            } else {
                DuckString::new(&mut *mode_data_ptr.add(row_idx))
                    .as_str()
                    .to_string()
            };
            let mode = TransformMode::from_str(&mode_str);

            let (bucket, key) = parse_s3_path(&s3_path)?;
            tasks.push((row_idx, Some((bucket, key, mode))));
        }

        let results: Vec<(usize, Result<String, String>)> = state.runtime.block_on(async {
            stream::iter(tasks)
                .map(|(row_idx, path_opt)| {
                    let state = state.clone();
                    async move {
                        match path_opt {
                            None => (row_idx, Ok(String::new())),
                            Some((bucket, key, mode)) => {
                                let client = match state.get_or_create_client(&bucket) {
                                    Ok(c) => c,
                                    Err(e) => return (row_idx, Err(e.to_string())),
                                };
                                let result = fetch_and_transform_text_with_mode(&client, &key, mode).await;
                                (row_idx, result.map_err(|e| e.to_string()))
                            }
                        }
                    }
                })
                .buffer_unordered(MAX_CONCURRENT_REQUESTS)
                .collect()
                .await
        });

        for (row_idx, result) in results {
            match result {
                Ok(text) => output_vector.insert(row_idx, text.as_str()),
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
    }

    fn volatile() -> bool {
        true
    }
}

pub(crate) struct S3TransformScript;

impl VScalar for S3TransformScript {
    type State = ExtensionState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let size = input.len();
        let path_vector = input.flat_vector(0);
        let script_vector = input.flat_vector(1);
        let output_vector = output.flat_vector();


        let path_data_ptr = path_vector.as_mut_ptr::<duckdb_string_t>();
        let script_data_ptr = script_vector.as_mut_ptr::<duckdb_string_t>();

        let mut tasks: Vec<(usize, Option<(String, String, AST)>)> = Vec::with_capacity(size);
        for row_idx in 0..size {
            if path_vector.row_is_null(row_idx as u64) {
                tasks.push((row_idx, None));
                continue;
            }

            let s3_path = DuckString::new(&mut *path_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let script = if script_vector.row_is_null(row_idx as u64) {
                "text".to_string()
            } else {
                DuckString::new(&mut *script_data_ptr.add(row_idx))
                    .as_str()
                    .to_string()
            };

            let ast = state.get_or_compile_script(&script)?;
            let (bucket, key) = parse_s3_path(&s3_path)?;
            tasks.push((row_idx, Some((bucket, key, ast))));
        }

        let results: Vec<(usize, Result<String, String>)> = state.runtime.block_on(async {
            stream::iter(tasks)
                .map(|(row_idx, path_opt)| {
                    let state = state.clone();
                    async move {
                        match path_opt {
                            None => (row_idx, Ok(String::new())),
                            Some((bucket, key, ast)) => {
                                let client = match state.get_or_create_client(&bucket) {
                                    Ok(c) => c,
                                    Err(e) => return (row_idx, Err(e.to_string())),
                                };
                                let text = match fetch_text(&client, &key).await {
                                    Ok(t) => t,
                                    Err(e) => return (row_idx, Err(e.to_string())),
                                };

                                let state_clone = state.clone();
                                let result = tokio::task::spawn_blocking(move || {
                                    state_clone.run_script(&text, &ast)
                                        .map_err(|e| e.to_string())
                                })
                                .await
                                .map_err(|e| e.to_string())
                                .and_then(|r| r);

                                (row_idx, result)
                            }
                        }
                    }
                })
                .buffer_unordered(MAX_CONCURRENT_REQUESTS)
                .collect()
                .await
        });

        for (row_idx, result) in results {
            match result {
                Ok(text) => output_vector.insert(row_idx, text.as_str()),
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
    }

    fn volatile() -> bool {
        true
    }
}

pub(crate) struct S3Fetch;

impl VScalar for S3Fetch {
    type State = ExtensionState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let size = input.len();
        let input_vector = input.flat_vector(0);
        let output_vector = output.flat_vector();


        let input_data_ptr = input_vector.as_mut_ptr::<duckdb_string_t>();

        let mut tasks: Vec<(usize, Option<(String, String)>)> = Vec::with_capacity(size);
        for row_idx in 0..size {
            if input_vector.row_is_null(row_idx as u64) {
                tasks.push((row_idx, None));
                continue;
            }

            let s3_path = DuckString::new(&mut *input_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let (bucket, key) = parse_s3_path(&s3_path)?;
            tasks.push((row_idx, Some((bucket, key))));
        }

        let results: Vec<(usize, Result<String, String>)> = state.runtime.block_on(async {
            stream::iter(tasks)
                .map(|(row_idx, path_opt)| {
                    let state = state.clone();
                    async move {
                        match path_opt {
                            None => (row_idx, Ok(String::new())),
                            Some((bucket, key)) => {
                                let client = match state.get_or_create_client(&bucket) {
                                    Ok(c) => c,
                                    Err(e) => return (row_idx, Err(e.to_string())),
                                };
                                let result = fetch_text(&client, &key).await;
                                (row_idx, result.map_err(|e| e.to_string()))
                            }
                        }
                    }
                })
                .buffer_unordered(MAX_CONCURRENT_REQUESTS)
                .collect()
                .await
        });

        for (row_idx, result) in results {
            match result {
                Ok(text) => output_vector.insert(row_idx, text.as_str()),
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
    }

    fn volatile() -> bool {
        true
    }
}

pub(crate) struct S3Exists;

impl VScalar for S3Exists {
    type State = ExtensionState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let size = input.len();
        let input_vector = input.flat_vector(0);
        let output_vector = output.flat_vector();


        let input_data_ptr = input_vector.as_mut_ptr::<duckdb_string_t>();

        let mut tasks: Vec<(usize, Option<(String, String)>)> = Vec::with_capacity(size);
        for row_idx in 0..size {
            if input_vector.row_is_null(row_idx as u64) {
                tasks.push((row_idx, None));
                continue;
            }

            let s3_path = DuckString::new(&mut *input_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let (bucket, key) = parse_s3_path(&s3_path)?;
            tasks.push((row_idx, Some((bucket, key))));
        }

        let results: Vec<(usize, Result<bool, String>)> = state.runtime.block_on(async {
            stream::iter(tasks)
                .map(|(row_idx, path_opt)| {
                    let state = state.clone();
                    async move {
                        match path_opt {
                            None => (row_idx, Ok(false)),
                            Some((bucket, key)) => {
                                let client = match state.get_or_create_client(&bucket) {
                                    Ok(c) => c,
                                    Err(e) => return (row_idx, Err(e.to_string())),
                                };
                                let path = ObjectPath::from(key.as_str());
                                match client.get(&path).await {
                                    Ok(_) => (row_idx, Ok(true)),
                                    Err(object_store::Error::NotFound { .. }) => (row_idx, Ok(false)),
                                    Err(e) => (row_idx, Err(format!("s3://{}/{}: {}", bucket, key, e))),
                                }
                            }
                        }
                    }
                })
                .buffer_unordered(MAX_CONCURRENT_REQUESTS)
                .collect()
                .await
        });

        let bool_output = output_vector.as_mut_ptr::<bool>();
        for (row_idx, result) in results {
            match result {
                Ok(exists) => *bool_output.add(row_idx) = exists,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
            LogicalTypeHandle::from(LogicalTypeId::Boolean),
        )]
    }

    fn volatile() -> bool {
        true
    }
}

pub(crate) struct S3Put;

impl VScalar for S3Put {
    type State = ExtensionState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let size = input.len();
        let path_vector = input.flat_vector(0);
        let content_vector = input.flat_vector(1);
        let output_vector = output.flat_vector();


        let path_data_ptr = path_vector.as_mut_ptr::<duckdb_string_t>();
        let content_data_ptr = content_vector.as_mut_ptr::<duckdb_string_t>();

        let mut tasks: Vec<(usize, Option<(String, String, String)>)> = Vec::with_capacity(size);
        for row_idx in 0..size {
            if path_vector.row_is_null(row_idx as u64) || content_vector.row_is_null(row_idx as u64) {
                tasks.push((row_idx, None));
                continue;
            }

            let s3_path = DuckString::new(&mut *path_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let content = DuckString::new(&mut *content_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let (bucket, key) = parse_s3_path(&s3_path)?;
            tasks.push((row_idx, Some((bucket, key, content))));
        }

        let results: Vec<(usize, Result<String, String>)> = state.runtime.block_on(async {
            stream::iter(tasks)
                .map(|(row_idx, task_opt)| {
                    let state = state.clone();
                    async move {
                        match task_opt {
                            None => (row_idx, Ok(String::new())),
                            Some((bucket, key, content)) => {
                                let client = match state.get_or_create_client(&bucket) {
                                    Ok(c) => c,
                                    Err(e) => return (row_idx, Err(e.to_string())),
                                };
                                match put_text(&client, &key, &content).await {
                                    Ok(()) => (row_idx, Ok(format!("s3://{}/{}", bucket, key))),
                                    Err(e) => (row_idx, Err(e.to_string())),
                                }
                            }
                        }
                    }
                })
                .buffer_unordered(MAX_CONCURRENT_REQUESTS)
                .collect()
                .await
        });

        for (row_idx, result) in results {
            match result {
                Ok(path) => output_vector.insert(row_idx, path.as_str()),
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
    }

    fn volatile() -> bool {
        true
    }
}
