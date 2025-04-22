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

use crate::logical_plan::producer::literal::to_substrait_literal;
use crate::logical_plan::producer::named_struct::to_substrait_named_struct;
use crate::logical_plan::producer::substrait_producer::SubstraitProducer;
use datafusion::common::substrait_datafusion_err;
use datafusion::logical_expr::{Expr, Values};
use substrait::proto::expression::literal::Struct;
use substrait::proto::read_rel::{ReadType, VirtualTable};
use substrait::proto::rel::RelType;
use substrait::proto::{ReadRel, Rel};

pub fn from_values(
    producer: &mut impl SubstraitProducer,
    v: &Values,
) -> datafusion::common::Result<Box<Rel>> {
    let values = v
        .values
        .iter()
        .map(|row| {
            let fields = row
                .iter()
                .map(|v| match v {
                    Expr::Literal(sv) => to_substrait_literal(producer, sv),
                    Expr::Alias(alias) => match alias.expr.as_ref() {
                        // The schema gives us the names, so we can skip aliases
                        Expr::Literal(sv) => to_substrait_literal(producer, sv),
                        _ => Err(substrait_datafusion_err!(
                                    "Only literal types can be aliased in Virtual Tables, got: {}", alias.expr.variant_name()
                                )),
                    },
                    _ => Err(substrait_datafusion_err!(
                                "Only literal types and aliases are supported in Virtual Tables, got: {}", v.variant_name()
                            )),
                })
                .collect::<datafusion::common::Result<_>>()?;
            Ok(Struct { fields })
        })
        .collect::<datafusion::common::Result<_>>()?;
    #[allow(deprecated)]
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Read(Box::new(ReadRel {
            common: None,
            base_schema: Some(to_substrait_named_struct(&v.schema)?),
            filter: None,
            best_effort_filter: None,
            projection: None,
            advanced_extension: None,
            read_type: Some(ReadType::VirtualTable(VirtualTable {
                values,
                expressions: vec![],
            })),
        }))),
    }))
}
