use std::io::Read;
use std::{collections::HashMap, fs::File};

use arrow2::{
    array::{Array, StructArray},
    chunk::Chunk,
    datatypes::Schema,
    io::ipc::write::WriteOptions,
};
use arrow2_convert::{serialize::TryIntoArrow, ArrowField};
use serde::Deserialize;

// Types for which I want to use arrow2-convert
pub type AssignmentValue = f64;

#[derive(Debug, Clone, ArrowField, Deserialize)]
pub struct Assignment {
    pub name: String,
    pub value: AssignmentValue,
}

#[derive(Debug, Clone, ArrowField, Deserialize)]
pub struct Action {
    pub target: Assignment,
    pub assignments: Vec<Assignment>,
}

pub type Plan = Vec<Action>;
pub type State = Vec<Assignment>;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // read into Plan
    let mut file = File::open("data/plan.yaml")?;
    let mut contents = vec![];
    file.read_to_end(&mut contents).unwrap();
    let plan: Plan = serde_yaml::from_slice(&contents)?;

    // infer schema by construting default Plan
    let default = Plan::default();
    let default_arrow_array: Box<dyn Array> = default.try_into_arrow()?;
    let datatype = default_arrow_array.data_type();
    let fields = StructArray::get_fields(&datatype);
    let schema = Schema::from(fields.to_vec());

    // into arrow
    let plan: Box<dyn Array> = plan.try_into_arrow()?;

    // downcast to StructArray
    let struct_plan = plan
        .as_any()
        .downcast_ref::<arrow2::array::StructArray>()
        .unwrap();

    // unsure how to get rid of this...
    let struct_plan = struct_plan.clone();

    let chunk = Chunk::new(vec![struct_plan.arced()]);

    let options = WriteOptions::default();

    // Serialization
    let serialized_schema = arrow2::io::flight::serialize_schema(&schema, None);

    let (_, serialized_batch) = arrow2::io::flight::serialize_batch(&chunk, &[], &options);

    // ... network ...

    // Deserialization
    let (schema, ipc_schema) =
        arrow2::io::flight::deserialize_schemas(&serialized_schema.data_header)?;

    let dictionaries_by_field = HashMap::new();

    let chunk = match arrow2::io::flight::deserialize_batch(
        &serialized_batch,
        &schema.fields,
        &ipc_schema,
        &dictionaries_by_field,
    ) {
        Ok(chunk) => chunk,
        Err(e) => panic!("not able to deserialize message: {:#?}", e),
    };

    println!("chunk: :{:#?}", chunk);

    Ok(())
}
