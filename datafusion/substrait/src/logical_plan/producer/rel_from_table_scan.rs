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

use crate::logical_plan::producer::named_struct::to_substrait_named_struct;
use crate::logical_plan::producer::substrait_producer::SubstraitProducer;
use datafusion::common::{DFSchema, ToDFSchema};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::TableScan;
use std::sync::Arc;
use substrait::proto::expression::mask_expression::{StructItem, StructSelect};
use substrait::proto::expression::MaskExpression;
use substrait::proto::read_rel::{NamedTable, ReadType};
use substrait::proto::rel::RelType;
use substrait::proto::{ReadRel, Rel};

pub fn from_table_scan(
    producer: &mut impl SubstraitProducer,
    scan: &TableScan,
) -> datafusion::common::Result<Box<Rel>> {
    let projection = scan.projection.as_ref().map(|p| {
        p.iter()
            .map(|i| StructItem {
                field: *i as i32,
                child: None,
            })
            .collect()
    });

    let projection = projection.map(|struct_items| MaskExpression {
        select: Some(StructSelect { struct_items }),
        maintain_singular_struct: false,
    });

    let table_schema = scan.source.schema().to_dfschema_ref()?;
    let base_schema = to_substrait_named_struct(&table_schema)?;

    let filter_option = if scan.filters.is_empty() {
        None
    } else {
        let table_schema_qualified = Arc::new(
            DFSchema::try_from_qualified_schema(
                scan.table_name.clone(),
                &(scan.source.schema()),
            )
            .unwrap(),
        );

        let combined_expr = conjunction(scan.filters.clone()).unwrap();
        let filter_expr =
            producer.handle_expr(&combined_expr, &table_schema_qualified)?;
        Some(Box::new(filter_expr))
    };

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Read(Box::new(ReadRel {
            common: None,
            base_schema: Some(base_schema),
            filter: filter_option,
            best_effort_filter: None,
            projection,
            advanced_extension: None,
            read_type: Some(ReadType::NamedTable(NamedTable {
                names: scan.table_name.to_vec(),
                advanced_extension: None,
            })),
        }))),
    }))
}
