use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    types::DuckString,
    vtab::arrow::WritableVector,
    vscalar::{ScalarFunctionSignature, VScalar},
};
use libduckdb_sys::duckdb_string_t;
use std::error::Error;

use crate::state::ExtensionState;

pub(crate) struct RhaiTransform;

impl VScalar for RhaiTransform {
    type State = ExtensionState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let size = input.len();
        let text_vector = input.flat_vector(0);
        let script_vector = input.flat_vector(1);
        let output_vector = output.flat_vector();

        let text_data_ptr = text_vector.as_mut_ptr::<duckdb_string_t>();
        let script_data_ptr = script_vector.as_mut_ptr::<duckdb_string_t>();

        for row_idx in 0..size {
            if text_vector.row_is_null(row_idx as u64) {
                output_vector.insert(row_idx, "");
                continue;
            }

            let text = DuckString::new(&mut *text_data_ptr.add(row_idx))
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
            let result = state.run_script(&text, &ast)?;
            output_vector.insert(row_idx, result.as_str());
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
        false
    }
}
