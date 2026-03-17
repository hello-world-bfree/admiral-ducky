mod chunking;
mod filters;
mod hierarchy;
mod parser;
mod types;

pub use parser::extract_and_chunk;
pub use types::{ChunkMode, ExtractConfig, ExtractionResult, FilterConfig};

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, RwLock};

#[repr(C)]
pub struct HtmlExtractBindData {
    html: String,
    config: ExtractConfig,
    mode_is_raw: bool,
}

#[repr(C)]
pub struct HtmlExtractInitData {
    result: RwLock<Option<ExtractionResult>>,
    current_row: AtomicUsize,
    executed: AtomicBool,
}

pub struct HtmlExtractVTab;

impl VTab for HtmlExtractVTab {
    type InitData = HtmlExtractInitData;
    type BindData = HtmlExtractBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let html = bind.get_parameter(0).to_string();
        let config_str = bind.get_parameter(1).to_string();

        let config = if config_str.is_empty() || config_str == "NULL" {
            ExtractConfig::default()
        } else if config_str.starts_with('{') {
            ExtractConfig::from_json(&config_str)?
        } else {
            ExtractConfig {
                mode: ChunkMode::from_str(&config_str),
                filters: FilterConfig::default(),
            }
        };

        let mode_is_raw = matches!(config.mode, ChunkMode::Raw);

        if mode_is_raw {
            bind.add_result_column("position", LogicalTypeHandle::from(LogicalTypeId::Integer));
            bind.add_result_column("element_type", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("text", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("word_count", LogicalTypeHandle::from(LogicalTypeId::Integer));
            bind.add_result_column("h1", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("h2", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("h3", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("h4", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("h5", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("h6", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("classes", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("error", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        } else {
            bind.add_result_column("chunk_id", LogicalTypeHandle::from(LogicalTypeId::Integer));
            bind.add_result_column("text", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("h1", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("h2", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("h3", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("h4", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("h5", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("h6", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            bind.add_result_column("word_count", LogicalTypeHandle::from(LogicalTypeId::Integer));
            bind.add_result_column("paragraph_count", LogicalTypeHandle::from(LogicalTypeId::Integer));
            bind.add_result_column("error", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        }

        Ok(HtmlExtractBindData {
            html,
            config,
            mode_is_raw,
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(HtmlExtractInitData {
            result: RwLock::new(None),
            current_row: AtomicUsize::new(0),
            executed: AtomicBool::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        if !init_data.executed.swap(true, Ordering::Relaxed) {
            let result = extract_and_chunk(&bind_data.html, &bind_data.config);
            let mut result_guard = init_data
                .result
                .write()
                .map_err(|e| format!("Lock error: {}", e))?;
            *result_guard = Some(result);
        }

        let result_guard = init_data
            .result
            .read()
            .map_err(|e| format!("Lock error: {}", e))?;

        let result = result_guard.as_ref().ok_or("No extraction result")?;

        if let Some(ref error) = result.parse_error {
            let current = init_data.current_row.load(Ordering::Relaxed);
            if current == 0 {
                if bind_data.mode_is_raw {
                    output.flat_vector(0).insert(0, "");
                    output.flat_vector(1).insert(0, "");
                    output.flat_vector(2).insert(0, "");
                    output.flat_vector(3).as_mut_slice::<i32>()[0] = 0;
                    output.flat_vector(4).insert(0, "");
                    output.flat_vector(5).insert(0, "");
                    output.flat_vector(6).insert(0, "");
                    output.flat_vector(7).insert(0, "");
                    output.flat_vector(8).insert(0, "");
                    output.flat_vector(9).insert(0, "");
                    output.flat_vector(10).insert(0, "");
                    output.flat_vector(11).insert(0, error.as_str());
                } else {
                    output.flat_vector(0).as_mut_slice::<i32>()[0] = 0;
                    output.flat_vector(1).insert(0, "");
                    output.flat_vector(2).insert(0, "");
                    output.flat_vector(3).insert(0, "");
                    output.flat_vector(4).insert(0, "");
                    output.flat_vector(5).insert(0, "");
                    output.flat_vector(6).insert(0, "");
                    output.flat_vector(7).insert(0, "");
                    output.flat_vector(8).as_mut_slice::<i32>()[0] = 0;
                    output.flat_vector(9).as_mut_slice::<i32>()[0] = 0;
                    output.flat_vector(10).insert(0, error.as_str());
                }
                output.set_len(1);
                init_data.current_row.store(1, Ordering::Relaxed);
            } else {
                output.set_len(0);
            }
            return Ok(());
        }

        let current = init_data.current_row.load(Ordering::Relaxed);

        if bind_data.mode_is_raw {
            if current >= result.raw_elements.len() {
                output.set_len(0);
                return Ok(());
            }

            let remaining = result.raw_elements.len() - current;
            let chunk_size = remaining.min(2048);

            for i in 0..chunk_size {
                let elem = &result.raw_elements[current + i];
                output.flat_vector(0).as_mut_slice::<i32>()[i] = elem.position;
                output.flat_vector(1).insert(i, elem.element_type.as_str());
                output.flat_vector(2).insert(i, elem.text.as_str());
                output.flat_vector(3).as_mut_slice::<i32>()[i] = elem.word_count;
                output.flat_vector(4).insert(i, elem.context.h1.as_deref().unwrap_or(""));
                output.flat_vector(5).insert(i, elem.context.h2.as_deref().unwrap_or(""));
                output.flat_vector(6).insert(i, elem.context.h3.as_deref().unwrap_or(""));
                output.flat_vector(7).insert(i, elem.context.h4.as_deref().unwrap_or(""));
                output.flat_vector(8).insert(i, elem.context.h5.as_deref().unwrap_or(""));
                output.flat_vector(9).insert(i, elem.context.h6.as_deref().unwrap_or(""));
                output.flat_vector(10).insert(i, elem.classes.as_str());
                output.flat_vector(11).insert(i, "");
            }

            init_data.current_row.store(current + chunk_size, Ordering::Relaxed);
            output.set_len(chunk_size);
        } else {
            if current >= result.chunks.len() {
                output.set_len(0);
                return Ok(());
            }

            let remaining = result.chunks.len() - current;
            let chunk_size = remaining.min(2048);

            for i in 0..chunk_size {
                let chunk = &result.chunks[current + i];
                output.flat_vector(0).as_mut_slice::<i32>()[i] = chunk.chunk_id;
                output.flat_vector(1).insert(i, chunk.text.as_str());
                output.flat_vector(2).insert(i, chunk.context.h1.as_deref().unwrap_or(""));
                output.flat_vector(3).insert(i, chunk.context.h2.as_deref().unwrap_or(""));
                output.flat_vector(4).insert(i, chunk.context.h3.as_deref().unwrap_or(""));
                output.flat_vector(5).insert(i, chunk.context.h4.as_deref().unwrap_or(""));
                output.flat_vector(6).insert(i, chunk.context.h5.as_deref().unwrap_or(""));
                output.flat_vector(7).insert(i, chunk.context.h6.as_deref().unwrap_or(""));
                output.flat_vector(8).as_mut_slice::<i32>()[i] = chunk.word_count;
                output.flat_vector(9).as_mut_slice::<i32>()[i] = chunk.paragraph_count;
                output.flat_vector(10).insert(i, chunk.error.as_deref().unwrap_or(""));
            }

            init_data.current_row.store(current + chunk_size, Ordering::Relaxed);
            output.set_len(chunk_size);
        }

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // html content
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // config (mode or JSON)
        ])
    }
}
