use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    types::DuckString,
    vtab::arrow::WritableVector,
    vscalar::{ScalarFunctionSignature, VScalar},
};
use futures::stream::{self, StreamExt};
use libduckdb_sys::duckdb_string_t;
use std::error::Error;

use crate::state::{ExtensionState, MAX_CONCURRENT_REQUESTS};

pub mod retrieval {
    tonic::include_proto!("retrieval.v1");
}

pub(crate) struct Embed;

impl VScalar for Embed {
    type State = ExtensionState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        use retrieval::catholic_embedding_service_client::CatholicEmbeddingServiceClient;
        use retrieval::EmbeddingRequest;

        let size = input.len();
        let text_vector = input.flat_vector(0);
        let endpoint_vector = input.flat_vector(1);


        let text_data_ptr = text_vector.as_mut_ptr::<duckdb_string_t>();
        let endpoint_data_ptr = endpoint_vector.as_mut_ptr::<duckdb_string_t>();

        let mut tasks: Vec<(usize, Option<(String, String)>)> = Vec::with_capacity(size);
        for row_idx in 0..size {
            if text_vector.row_is_null(row_idx as u64) {
                tasks.push((row_idx, None));
                continue;
            }

            let text = DuckString::new(&mut *text_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let endpoint = if endpoint_vector.row_is_null(row_idx as u64) {
                "http://localhost:50051".to_string()
            } else {
                DuckString::new(&mut *endpoint_data_ptr.add(row_idx))
                    .as_str()
                    .to_string()
            };

            tasks.push((row_idx, Some((text, endpoint))));
        }

        let results: Vec<(usize, Result<Vec<f64>, String>)> = state.runtime.block_on(async {
            stream::iter(tasks)
                .map(|(row_idx, task_opt)| {
                    let state = state.clone();
                    async move {
                        match task_opt {
                            None => (row_idx, Ok(vec![])),
                            Some((text, endpoint)) => {
                                let max_retries = 3;
                                let mut last_err = String::new();
                                let mut succeeded = None;

                                for attempt in 0..max_retries {
                                    if attempt > 0 {
                                        tokio::time::sleep(tokio::time::Duration::from_millis(500 * (1 << attempt))).await;
                                    }

                                    let channel = match state.get_or_create_channel(&endpoint).await {
                                        Ok(c) => c,
                                        Err(e) => {
                                            last_err = e.to_string();
                                            continue;
                                        }
                                    };

                                    let mut client = CatholicEmbeddingServiceClient::new(channel);
                                    let request = tonic::Request::new(EmbeddingRequest { input: text.clone() });

                                    match client.embedding(request).await {
                                        Ok(response) => {
                                            succeeded = Some(response.into_inner().embedding);
                                            break;
                                        }
                                        Err(e) => {
                                            last_err = format!("gRPC error: {}", e);
                                            let code = e.code();
                                            if code == tonic::Code::Unavailable
                                                || code == tonic::Code::Unknown
                                                || code == tonic::Code::Internal
                                            {
                                                state.drop_channel(&endpoint);
                                                continue;
                                            }
                                            return (row_idx, Err(last_err));
                                        }
                                    }
                                }

                                match succeeded {
                                    Some(embedding) => (row_idx, Ok(embedding)),
                                    None => (row_idx, Err(last_err)),
                                }
                            }
                        }
                    }
                })
                .buffer_unordered(4)
                .collect()
                .await
        });

        let mut sorted_results = results;
        sorted_results.sort_by_key(|(idx, _)| *idx);

        let mut validated: Vec<(usize, Vec<f64>)> = Vec::with_capacity(sorted_results.len());
        for (row_idx, result) in sorted_results {
            match result {
                Ok(embedding) => validated.push((row_idx, embedding)),
                Err(e) => return Err(e.into()),
            }
        }

        let total_elements: usize = validated.iter().map(|(_, v)| v.len()).sum();
        let mut list_vector = output.list_vector();
        let mut child = list_vector.child(total_elements);
        let child_slice = child.as_mut_slice::<f64>();

        let mut offset: usize = 0;
        for (row_idx, embedding) in &validated {
            let len = embedding.len();
            list_vector.set_entry(*row_idx, offset, len);
            for (i, val) in embedding.iter().enumerate() {
                child_slice[offset + i] = *val;
            }
            offset += len;
        }

        list_vector.set_len(total_elements);

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ],
            LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Double)),
        )]
    }

    fn volatile() -> bool {
        true
    }
}

pub(crate) struct EmbedFake;

impl VScalar for EmbedFake {
    type State = ExtensionState;

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let size = input.len();
        let text_vector = input.flat_vector(0);
        let dim_vector = input.flat_vector(1);

        let text_data_ptr = text_vector.as_mut_ptr::<duckdb_string_t>();
        let dim_ptr = dim_vector.as_mut_ptr::<i32>();

        let mut validated: Vec<(usize, Vec<f64>)> = Vec::with_capacity(size);
        for row_idx in 0..size {
            if text_vector.row_is_null(row_idx as u64) {
                validated.push((row_idx, vec![]));
                continue;
            }

            let text = DuckString::new(&mut *text_data_ptr.add(row_idx))
                .as_str()
                .to_string();
            let dim = *dim_ptr.add(row_idx) as usize;

            let mut hash: f64 = 0.0;
            for (i, b) in text.bytes().enumerate() {
                hash += (b as f64) * ((i + 1) as f64) * 0.001;
            }

            let embedding: Vec<f64> = (0..dim)
                .map(|i| ((hash + i as f64) * 0.01).sin())
                .collect();
            validated.push((row_idx, embedding));
        }

        let total_elements: usize = validated.iter().map(|(_, v)| v.len()).sum();
        let mut list_vector = output.list_vector();
        let mut child = list_vector.child(total_elements);
        let child_slice = child.as_mut_slice::<f64>();

        let mut offset: usize = 0;
        for (row_idx, embedding) in &validated {
            let len = embedding.len();
            list_vector.set_entry(*row_idx, offset, len);
            for (i, val) in embedding.iter().enumerate() {
                child_slice[offset + i] = *val;
            }
            offset += len;
        }

        list_vector.set_len(total_elements);

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Integer),
            ],
            LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Double)),
        )]
    }
}

pub(crate) struct TokenCount;

impl VScalar for TokenCount {
    type State = ExtensionState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        use retrieval::catholic_embedding_service_client::CatholicEmbeddingServiceClient;
        use retrieval::CountTokensRequest;

        let size = input.len();
        let text_vector = input.flat_vector(0);
        let endpoint_vector = input.flat_vector(1);
        let output_vector = output.flat_vector();


        let text_data_ptr = text_vector.as_mut_ptr::<duckdb_string_t>();
        let endpoint_data_ptr = endpoint_vector.as_mut_ptr::<duckdb_string_t>();

        let mut tasks: Vec<(usize, Option<(String, String)>)> = Vec::with_capacity(size);
        for row_idx in 0..size {
            if text_vector.row_is_null(row_idx as u64) {
                tasks.push((row_idx, None));
                continue;
            }

            let text = DuckString::new(&mut *text_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let endpoint = if endpoint_vector.row_is_null(row_idx as u64) {
                "http://localhost:50051".to_string()
            } else {
                DuckString::new(&mut *endpoint_data_ptr.add(row_idx))
                    .as_str()
                    .to_string()
            };

            tasks.push((row_idx, Some((text, endpoint))));
        }

        let results: Vec<(usize, Result<u32, String>)> = state.runtime.block_on(async {
            stream::iter(tasks)
                .map(|(row_idx, task_opt)| {
                    let state = state.clone();
                    async move {
                        match task_opt {
                            None => (row_idx, Ok(0u32)),
                            Some((text, endpoint)) => {
                                let channel = match state.get_or_create_channel(&endpoint).await {
                                    Ok(c) => c,
                                    Err(e) => return (row_idx, Err(e.to_string())),
                                };

                                let mut client = CatholicEmbeddingServiceClient::new(channel);
                                let request = tonic::Request::new(CountTokensRequest { input: text });

                                match client.count_tokens(request).await {
                                    Ok(response) => (row_idx, Ok(response.into_inner().tokens)),
                                    Err(e) => (row_idx, Err(format!("gRPC error: {}", e))),
                                }
                            }
                        }
                    }
                })
                .buffer_unordered(MAX_CONCURRENT_REQUESTS)
                .collect()
                .await
        });

        let uint_output = output_vector.as_mut_ptr::<u32>();
        for (row_idx, result) in results {
            match result {
                Ok(tokens) => *uint_output.add(row_idx) = tokens,
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
            LogicalTypeHandle::from(LogicalTypeId::UInteger),
        )]
    }

    fn volatile() -> bool {
        true
    }
}

async fn count_tokens_grpc(
    state: &ExtensionState,
    endpoint: &str,
    text: &str,
) -> Result<u32, String> {
    use retrieval::catholic_embedding_service_client::CatholicEmbeddingServiceClient;
    use retrieval::CountTokensRequest;

    let max_retries = 3;
    let mut last_err = String::new();

    for attempt in 0..max_retries {
        if attempt > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(500 * (1 << attempt))).await;
        }

        let channel = state
            .get_or_create_channel(endpoint)
            .await
            .map_err(|e| e.to_string())?;
        let mut client = CatholicEmbeddingServiceClient::new(channel);
        let request = tonic::Request::new(CountTokensRequest {
            input: text.to_string(),
        });

        match client.count_tokens(request).await {
            Ok(r) => return Ok(r.into_inner().tokens),
            Err(e) => {
                last_err = format!("gRPC error: {}", e);
                let code = e.code();
                if code == tonic::Code::Unavailable
                    || code == tonic::Code::Unknown
                    || code == tonic::Code::Internal
                {
                    state.drop_channel(endpoint);
                    continue;
                }
                return Err(last_err);
            }
        }
    }

    Err(last_err)
}

fn char_boundary_at_or_after(text: &str, pos: usize) -> usize {
    let mut i = pos;
    while i < text.len() && !text.is_char_boundary(i) {
        i += 1;
    }
    i
}

fn split_at_midpoint(text: &str) -> (&str, &str) {
    let mid = char_boundary_at_or_after(text, text.len() / 2);
    if let Some(pos) = text[mid..].find(|c: char| c == '.' || c == '!' || c == '?') {
        let split = mid + pos + 1;
        let split = char_boundary_at_or_after(text, split);
        let right_start = if split < text.len() && text[split..].starts_with(' ') {
            split + 1
        } else {
            split
        };
        (&text[..split], &text[right_start..])
    } else if let Some(pos) = text[..mid].rfind(|c: char| c == '.' || c == '!' || c == '?') {
        let split = pos + 1;
        let split = char_boundary_at_or_after(text, split);
        let right_start = if split < text.len() && text[split..].starts_with(' ') {
            split + 1
        } else {
            split
        };
        (&text[..split], &text[right_start..])
    } else if let Some(pos) = text[mid..].find(' ') {
        let split = mid + pos;
        (&text[..split], &text[split + 1..])
    } else {
        (&text[..mid], &text[mid..])
    }
}

fn chunk_recursive<'a>(
    state: &'a ExtensionState,
    endpoint: &'a str,
    text: &'a str,
    max_tokens: u32,
) -> futures::future::BoxFuture<'a, Result<Vec<String>, String>> {
    Box::pin(async move {
        let tokens = count_tokens_grpc(state, endpoint, text).await?;

        if tokens <= max_tokens {
            return Ok(vec![text.to_string()]);
        }

        let (left, right) = split_at_midpoint(text);

        if left.is_empty() || right.is_empty() {
            return Ok(vec![text.to_string()]);
        }

        let mut chunks = chunk_recursive(state, endpoint, left, max_tokens).await?;
        let right_chunks = chunk_recursive(state, endpoint, right, max_tokens).await?;
        chunks.extend(right_chunks);

        Ok(chunks)
    })
}

pub(crate) struct ChunkText;

impl VScalar for ChunkText {
    type State = ExtensionState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let size = input.len();
        let text_vector = input.flat_vector(0);
        let max_tokens_vector = input.flat_vector(1);
        let endpoint_vector = input.flat_vector(2);


        let text_data_ptr = text_vector.as_mut_ptr::<duckdb_string_t>();
        let max_tokens_ptr = max_tokens_vector.as_mut_ptr::<i32>();
        let endpoint_data_ptr = endpoint_vector.as_mut_ptr::<duckdb_string_t>();

        let mut row_inputs: Vec<(usize, Option<(String, u32, String)>)> = Vec::with_capacity(size);
        for row_idx in 0..size {
            if text_vector.row_is_null(row_idx as u64) {
                row_inputs.push((row_idx, None));
                continue;
            }

            let text = DuckString::new(&mut *text_data_ptr.add(row_idx))
                .as_str()
                .to_string();

            let max_tokens = *max_tokens_ptr.add(row_idx) as u32;

            let endpoint = if endpoint_vector.row_is_null(row_idx as u64) {
                "http://localhost:50051".to_string()
            } else {
                DuckString::new(&mut *endpoint_data_ptr.add(row_idx))
                    .as_str()
                    .to_string()
            };

            row_inputs.push((row_idx, Some((text, max_tokens, endpoint))));
        }

        let results: Vec<(usize, Result<Vec<String>, String>)> = state.runtime.block_on(async {
            let mut results = Vec::with_capacity(row_inputs.len());
            for (row_idx, opt) in row_inputs {
                match opt {
                    None => results.push((row_idx, Ok(vec![]))),
                    Some((text, max_tokens, endpoint)) => {
                        match chunk_recursive(&state, &endpoint, &text, max_tokens).await {
                            Ok(chunks) => results.push((row_idx, Ok(chunks))),
                            Err(e) => results.push((row_idx, Err(e))),
                        }
                    }
                }
            }
            results
        });

        let mut sorted_results = results;
        sorted_results.sort_by_key(|(idx, _)| *idx);

        let mut validated: Vec<(usize, Vec<String>)> = Vec::with_capacity(sorted_results.len());
        for (row_idx, result) in sorted_results {
            match result {
                Ok(chunks) => validated.push((row_idx, chunks)),
                Err(e) => return Err(e.into()),
            }
        }

        let total_strings: usize = validated.iter().map(|(_, v)| v.len()).sum();
        let mut list_vector = output.list_vector();
        let child = list_vector.child(total_strings);

        let mut offset: usize = 0;
        for (row_idx, chunks) in &validated {
            let len = chunks.len();
            list_vector.set_entry(*row_idx, offset, len);
            for (i, chunk) in chunks.iter().enumerate() {
                child.insert(offset + i, chunk.as_str());
            }
            offset += len;
        }

        list_vector.set_len(total_strings);

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Integer),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ],
            LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        )]
    }

    fn volatile() -> bool {
        true
    }
}
