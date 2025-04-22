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

use crate::logical_plan::producer::rex_from_field_ref::substrait_field_ref;
use crate::logical_plan::producer::substrait_producer::SubstraitProducer;
use datafusion::common::not_impl_err;
use datafusion::logical_expr::Distinct;
use substrait::proto::aggregate_rel::Grouping;
use substrait::proto::rel::RelType;
use substrait::proto::{AggregateRel, Rel};

pub fn from_distinct(
    producer: &mut impl SubstraitProducer,
    distinct: &Distinct,
) -> datafusion::common::Result<Box<Rel>> {
    match distinct {
        Distinct::All(plan) => {
            // Use Substrait's AggregateRel with empty measures to represent `select distinct`
            let input = producer.handle_plan(plan.as_ref())?;
            // Get grouping keys from the input relation's number of output fields
            let grouping = (0..plan.schema().fields().len())
                .map(substrait_field_ref)
                .collect::<datafusion::common::Result<Vec<_>>>()?;

            #[allow(deprecated)]
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Aggregate(Box::new(AggregateRel {
                    common: None,
                    input: Some(input),
                    grouping_expressions: vec![],
                    groupings: vec![Grouping {
                        grouping_expressions: grouping,
                        expression_references: vec![],
                    }],
                    measures: vec![],
                    advanced_extension: None,
                }))),
            }))
        }
        Distinct::On(_) => not_impl_err!("Cannot convert Distinct::On"),
    }
}
