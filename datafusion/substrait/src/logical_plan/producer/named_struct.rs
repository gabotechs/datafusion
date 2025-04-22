// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::logical_plan::producer::r#type::to_substrait_type;
use crate::logical_plan::producer::utils::flatten_names;
use crate::variation_const::DEFAULT_TYPE_VARIATION_REF;
use datafusion::common::DFSchemaRef;
use substrait::proto::{r#type, NamedStruct};

pub(super) fn to_substrait_named_struct(
    schema: &DFSchemaRef,
) -> datafusion::common::Result<NamedStruct> {
    let mut names = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        flatten_names(field, false, &mut names)?;
    }

    let field_types = r#type::Struct {
        types: schema
            .fields()
            .iter()
            .map(|f| to_substrait_type(f.data_type(), f.is_nullable()))
            .collect::<datafusion::common::Result<_>>()?,
        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
        nullability: r#type::Nullability::Required as i32,
    };

    Ok(NamedStruct {
        names,
        r#struct: Some(field_types),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::consumer::from_substrait_named_struct;
    use crate::logical_plan::producer::tests::test_consumer;
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion::common::DFSchema;
    use std::sync::Arc;

    #[test]
    fn named_struct_names() -> datafusion::common::Result<()> {
        let schema = DFSchemaRef::new(DFSchema::try_from(Schema::new(vec![
            Field::new("int", DataType::Int32, true),
            Field::new(
                "struct",
                DataType::Struct(Fields::from(vec![Field::new(
                    "inner",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                    true,
                )])),
                true,
            ),
            Field::new("trailer", DataType::Float64, true),
        ]))?);

        let named_struct = to_substrait_named_struct(&schema)?;

        // Struct field names should be flattened DFS style
        // List field names should be omitted
        assert_eq!(
            named_struct.names,
            vec!["int", "struct", "inner", "trailer"]
        );

        let roundtrip_schema =
            from_substrait_named_struct(&test_consumer(), &named_struct)?;
        assert_eq!(schema.as_ref(), &roundtrip_schema);
        Ok(())
    }
}
