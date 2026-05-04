use anyhow::Context;
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    types::DuckString,
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab, arrow::WritableVector},
    vscalar::{ScalarFunctionSignature, VScalar},
};
use futures::stream::{self, StreamExt};
use libduckdb_sys::duckdb_string_t;
use std::{
    error::Error,
    sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, RwLock},
};

use crate::state::{ExtensionState, MAX_CONCURRENT_REQUESTS};

pub(crate) struct HttpResponse {
    pub status_code: i32,
    pub body: String,
    pub headers: String,
}

pub(crate) fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

pub(crate) fn is_retryable_error(e: &reqwest::Error) -> bool {
    e.is_connect() || e.is_timeout()
}

pub(crate) async fn http_post_request(
    client: &reqwest::Client,
    url: &str,
    body: &str,
    headers: Option<&str>,
) -> Result<HttpResponse, Box<dyn Error + Send + Sync>> {
    let mut request = client.post(url).body(body.to_string());

    if let Some(headers_json) = headers {
        let parsed: serde_json::Value = serde_json::from_str(headers_json)
            .map_err(|e| format!("Invalid headers JSON: {}", e))?;

        if let serde_json::Value::Object(map) = parsed {
            for (key, value) in map {
                if let serde_json::Value::String(v) = value {
                    request = request.header(&key, &v);
                }
            }
        }
    }

    request = request.header("Content-Type", "application/json");

    let response = request.send().await?;
    let status_code = response.status().as_u16() as i32;

    let response_headers: Vec<String> = response
        .headers()
        .iter()
        .map(|(k, v)| format!("{}: {}", k, v.to_str().unwrap_or("")))
        .collect();
    let headers_str = response_headers.join("\n");

    let body = response.text().await?;

    Ok(HttpResponse {
        status_code,
        body,
        headers: headers_str,
    })
}

pub(crate) async fn http_post_with_retry(
    client: &reqwest::Client,
    url: &str,
    body: &str,
    headers: Option<&str>,
    max_retries: u32,
) -> Result<HttpResponse, Box<dyn Error + Send + Sync>> {
    let mut attempt = 0;
    loop {
        let result = http_post_request(client, url, body, headers).await;

        match result {
            Ok(response) => {
                if attempt < max_retries && is_retryable_status(reqwest::StatusCode::from_u16(response.status_code as u16).unwrap_or(reqwest::StatusCode::OK)) {
                    let delay = std::time::Duration::from_millis(100 * 2u64.pow(attempt));
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                    continue;
                }
                return Ok(response);
            }
            Err(e) => {
                let is_retryable = e.downcast_ref::<reqwest::Error>()
                    .map(is_retryable_error)
                    .unwrap_or(false);

                if attempt < max_retries && is_retryable {
                    let delay = std::time::Duration::from_millis(100 * 2u64.pow(attempt));
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                    continue;
                }
                return Err(e);
            }
        }
    }
}

pub(crate) struct HttpPost;

impl VScalar for HttpPost {
    type State = ExtensionState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let size = input.len();
        let url_vector = input.flat_vector(0);
        let body_vector = input.flat_vector(1);
        let has_headers = input.num_columns() > 2;
        let headers_vector = if has_headers { Some(input.flat_vector(2)) } else { None };

        let url_data_ptr = url_vector.as_mut_ptr::<duckdb_string_t>();
        let body_data_ptr = body_vector.as_mut_ptr::<duckdb_string_t>();
        let headers_data_ptr = headers_vector.as_ref().map(|v| v.as_mut_ptr::<duckdb_string_t>());

        let mut tasks: Vec<(usize, Option<(String, String, Option<String>)>)> = Vec::with_capacity(size);
        for row_idx in 0..size {
            if url_vector.row_is_null(row_idx as u64) || body_vector.row_is_null(row_idx as u64) {
                tasks.push((row_idx, None));
                continue;
            }

            let url = DuckString::new(&mut *url_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let body = DuckString::new(&mut *body_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let headers = if let Some(ptr) = headers_data_ptr {
                if let Some(ref hv) = headers_vector {
                    if !hv.row_is_null(row_idx as u64) {
                        Some(DuckString::new(&mut *ptr.add(row_idx)).as_str().to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

            tasks.push((row_idx, Some((url, body, headers))));
        }

        let results: Vec<(usize, Result<HttpResponse, String>)> = state.runtime.block_on(async {
            stream::iter(tasks)
                .map(|(row_idx, task_opt)| {
                    let client = state.http_client.clone();
                    async move {
                        match task_opt {
                            None => (row_idx, Ok(HttpResponse {
                                status_code: 0,
                                body: String::new(),
                                headers: String::new(),
                            })),
                            Some((url, body, headers)) => {
                                let result = http_post_with_retry(
                                    &client,
                                    &url,
                                    &body,
                                    headers.as_deref(),
                                    3,
                                ).await;
                                (row_idx, result.map_err(|e| e.to_string()))
                            }
                        }
                    }
                })
                .buffer_unordered(MAX_CONCURRENT_REQUESTS)
                .collect()
                .await
        });

        let mut sorted_results = results;
        sorted_results.sort_by_key(|(idx, _)| *idx);

        let struct_vector = output.struct_vector();
        let mut status_child = struct_vector.child(0, size);
        let body_child = struct_vector.child(1, size);
        let headers_child = struct_vector.child(2, size);

        let status_slice = status_child.as_mut_slice::<i32>();

        for (row_idx, result) in sorted_results {
            match result {
                Ok(response) => {
                    status_slice[row_idx] = response.status_code;
                    body_child.insert(row_idx, response.body.as_str());
                    headers_child.insert(row_idx, response.headers.as_str());
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                ],
                LogicalTypeHandle::struct_type(&[
                    ("status_code", LogicalTypeHandle::from(LogicalTypeId::Integer)),
                    ("body", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                    ("headers", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ]),
            ),
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                ],
                LogicalTypeHandle::struct_type(&[
                    ("status_code", LogicalTypeHandle::from(LogicalTypeId::Integer)),
                    ("body", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                    ("headers", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ]),
            ),
        ]
    }

    fn volatile() -> bool {
        true
    }
}

pub(crate) struct HttpPostRhai;

impl VScalar for HttpPostRhai {
    type State = ExtensionState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let size = input.len();
        let url_vector = input.flat_vector(0);
        let data_vector = input.flat_vector(1);
        let script_vector = input.flat_vector(2);
        let has_headers = input.num_columns() > 3;
        let headers_vector = if has_headers { Some(input.flat_vector(3)) } else { None };

        let url_data_ptr = url_vector.as_mut_ptr::<duckdb_string_t>();
        let data_data_ptr = data_vector.as_mut_ptr::<duckdb_string_t>();
        let script_data_ptr = script_vector.as_mut_ptr::<duckdb_string_t>();
        let headers_data_ptr = headers_vector.as_ref().map(|v| v.as_mut_ptr::<duckdb_string_t>());

        let mut tasks: Vec<(usize, Option<(String, String, Option<String>)>)> = Vec::with_capacity(size);
        for row_idx in 0..size {
            if url_vector.row_is_null(row_idx as u64) || data_vector.row_is_null(row_idx as u64) || script_vector.row_is_null(row_idx as u64) {
                tasks.push((row_idx, None));
                continue;
            }

            let url = DuckString::new(&mut *url_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let data = DuckString::new(&mut *data_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let script = DuckString::new(&mut *script_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let ast = state.get_or_compile_script(&script)?;
            let transformed_body = state.run_script(&data, &ast)?;

            let headers = if let Some(ptr) = headers_data_ptr {
                if let Some(ref hv) = headers_vector {
                    if !hv.row_is_null(row_idx as u64) {
                        Some(DuckString::new(&mut *ptr.add(row_idx)).as_str().to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

            tasks.push((row_idx, Some((url, transformed_body, headers))));
        }

        let results: Vec<(usize, Result<HttpResponse, String>)> = state.runtime.block_on(async {
            stream::iter(tasks)
                .map(|(row_idx, task_opt)| {
                    let client = state.http_client.clone();
                    async move {
                        match task_opt {
                            None => (row_idx, Ok(HttpResponse {
                                status_code: 0,
                                body: String::new(),
                                headers: String::new(),
                            })),
                            Some((url, body, headers)) => {
                                let result = http_post_with_retry(
                                    &client,
                                    &url,
                                    &body,
                                    headers.as_deref(),
                                    3,
                                ).await;
                                (row_idx, result.map_err(|e| e.to_string()))
                            }
                        }
                    }
                })
                .buffer_unordered(MAX_CONCURRENT_REQUESTS)
                .collect()
                .await
        });

        let mut sorted_results = results;
        sorted_results.sort_by_key(|(idx, _)| *idx);

        let struct_vector = output.struct_vector();
        let mut status_child = struct_vector.child(0, size);
        let body_child = struct_vector.child(1, size);
        let headers_child = struct_vector.child(2, size);

        let status_slice = status_child.as_mut_slice::<i32>();

        for (row_idx, result) in sorted_results {
            match result {
                Ok(response) => {
                    status_slice[row_idx] = response.status_code;
                    body_child.insert(row_idx, response.body.as_str());
                    headers_child.insert(row_idx, response.headers.as_str());
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                ],
                LogicalTypeHandle::struct_type(&[
                    ("status_code", LogicalTypeHandle::from(LogicalTypeId::Integer)),
                    ("body", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                    ("headers", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ]),
            ),
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                ],
                LogicalTypeHandle::struct_type(&[
                    ("status_code", LogicalTypeHandle::from(LogicalTypeId::Integer)),
                    ("body", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                    ("headers", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ]),
            ),
        ]
    }

    fn volatile() -> bool {
        true
    }
}

#[repr(C)]
pub(crate) struct HttpPostBatchBindData {
    pub url: String,
    pub payloads: Vec<String>,
    pub batch_size: usize,
    pub headers: Option<String>,
}

#[repr(C)]
pub(crate) struct HttpPostBatchInitData {
    pub current_batch: AtomicUsize,
    pub results: RwLock<Vec<BatchResult>>,
    pub executed: AtomicBool,
}

pub(crate) struct BatchResult {
    pub batch_id: i32,
    pub row_count: i32,
    pub status_code: i32,
    pub response_body: String,
}

pub(crate) struct HttpPostBatchVTab;

impl VTab for HttpPostBatchVTab {
    type InitData = HttpPostBatchInitData;
    type BindData = HttpPostBatchBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("batch_id", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("row_count", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("status_code", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("response_body", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let url = bind.get_parameter(0).to_string();
        let payloads_param = bind.get_parameter(1).to_string();
        let batch_size_param = bind.get_parameter(2).to_string();
        let headers = if bind.get_parameter(3).to_string().is_empty() {
            None
        } else {
            Some(bind.get_parameter(3).to_string())
        };

        let payloads_json = if payloads_param.ends_with(".json") || payloads_param.starts_with('/') || payloads_param.starts_with("./") {
            std::fs::read_to_string(&payloads_param)
                .map_err(|e| format!("Failed to read file '{}': {}", payloads_param, e))?
        } else {
            payloads_param
        };

        let payloads_value: serde_json::Value = serde_json::from_str(&payloads_json)
            .map_err(|e| format!("Invalid payloads JSON: {}", e))?;

        let payloads: Vec<String> = match payloads_value {
            serde_json::Value::Array(arr) => {
                arr.into_iter()
                    .map(|v| {
                        if let serde_json::Value::String(s) = v {
                            s
                        } else {
                            serde_json::to_string(&v).unwrap_or_default()
                        }
                    })
                    .collect()
            }
            _ => return Err("Payloads must be a JSON array".into()),
        };

        let batch_size: usize = batch_size_param.parse()
            .map_err(|e| format!("Invalid batch_size: {}", e))?;

        if batch_size == 0 {
            return Err("batch_size must be greater than 0".into());
        }

        Ok(HttpPostBatchBindData {
            url,
            payloads,
            batch_size,
            headers,
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(HttpPostBatchInitData {
            current_batch: AtomicUsize::new(0),
            results: RwLock::new(Vec::new()),
            executed: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        if !init_data.executed.swap(true, Ordering::Relaxed) {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .context("Failed to create Tokio runtime")?;

            let client = reqwest::Client::builder()
                .pool_max_idle_per_host(32)
                .build()
                .map_err(|e| format!("Failed to create HTTP client: {}", e))?;

            let batches: Vec<Vec<String>> = bind_data.payloads
                .chunks(bind_data.batch_size)
                .map(|chunk| chunk.to_vec())
                .collect();

            let batch_results: Vec<BatchResult> = runtime.block_on(async {
                stream::iter(batches.into_iter().enumerate())
                    .map(|(batch_idx, batch)| {
                        let client = client.clone();
                        let url = bind_data.url.clone();
                        let headers = bind_data.headers.clone();
                        async move {
                            let row_count = batch.len() as i32;
                            let batch_json = serde_json::to_string(&batch)
                                .unwrap_or_else(|_| "[]".to_string());

                            match http_post_with_retry(&client, &url, &batch_json, headers.as_deref(), 3).await {
                                Ok(response) => BatchResult {
                                    batch_id: batch_idx as i32,
                                    row_count,
                                    status_code: response.status_code,
                                    response_body: response.body,
                                },
                                Err(e) => BatchResult {
                                    batch_id: batch_idx as i32,
                                    row_count,
                                    status_code: -1,
                                    response_body: e.to_string(),
                                },
                            }
                        }
                    })
                    .buffer_unordered(MAX_CONCURRENT_REQUESTS)
                    .collect()
                    .await
            });

            let mut results = init_data.results.write().map_err(|e| format!("Lock error: {}", e))?;
            *results = batch_results;
            results.sort_by_key(|r| r.batch_id);
        }

        let results = init_data.results.read().map_err(|e| format!("Lock error: {}", e))?;
        let current = init_data.current_batch.load(Ordering::Relaxed);

        if current >= results.len() {
            output.set_len(0);
            return Ok(());
        }

        let remaining = results.len() - current;
        let chunk_size = remaining.min(2048);

        let mut batch_id_vector = output.flat_vector(0);
        let mut row_count_vector = output.flat_vector(1);
        let mut status_code_vector = output.flat_vector(2);
        let response_body_vector = output.flat_vector(3);

        let batch_id_slice = batch_id_vector.as_mut_slice::<i32>();
        let row_count_slice = row_count_vector.as_mut_slice::<i32>();
        let status_code_slice = status_code_vector.as_mut_slice::<i32>();

        for i in 0..chunk_size {
            let result = &results[current + i];
            batch_id_slice[i] = result.batch_id;
            row_count_slice[i] = result.row_count;
            status_code_slice[i] = result.status_code;
            response_body_vector.insert(i, result.response_body.as_str());
        }

        init_data.current_batch.store(current + chunk_size, Ordering::Relaxed);
        output.set_len(chunk_size);

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }
}
