use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    types::DuckString,
    vtab::arrow::WritableVector,
    vscalar::{ScalarFunctionSignature, VScalar},
};
use libduckdb_sys::duckdb_string_t;
use std::error::Error;
use whatlang::{detect, Lang};

use crate::state::ExtensionState;

pub(crate) struct IsEnglish;

impl VScalar for IsEnglish {
    type State = ExtensionState;

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let size = input.len();
        let input_vector = input.flat_vector(0);
        let mut output_vector = output.flat_vector();
        let bool_output = output_vector.as_mut_ptr::<bool>();
        let input_data_ptr = input_vector.as_mut_ptr::<duckdb_string_t>();

        for row_idx in 0..size {
            if input_vector.row_is_null(row_idx as u64) {
                output_vector.set_null(row_idx);
                continue;
            }

            let text = DuckString::new(&mut *input_data_ptr.add(row_idx))
                .as_str();

            let is_eng = match detect(&text) {
                Some(info) => info.lang() == Lang::Eng && info.is_reliable(),
                None => false,
            };

            *bool_output.add(row_idx) = is_eng;
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
        false
    }
}
