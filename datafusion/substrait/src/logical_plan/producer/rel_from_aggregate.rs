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

use crate::logical_plan::producer::substrait_producer::SubstraitProducer;
use crate::logical_plan::producer::{from_aggregate_function, to_substrait_groupings};
use datafusion::common::{internal_err, DFSchemaRef};
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{Aggregate, Expr};
use substrait::proto::aggregate_rel::Measure;
use substrait::proto::rel::RelType;
use substrait::proto::{AggregateRel, Rel};

pub fn from_aggregate(
    producer: &mut impl SubstraitProducer,
    agg: &Aggregate,
) -> datafusion::common::Result<Box<Rel>> {
    let input = producer.handle_plan(agg.input.as_ref())?;
    let (grouping_expressions, groupings) =
        to_substrait_groupings(producer, &agg.group_expr, agg.input.schema())?;
    let measures = agg
        .aggr_expr
        .iter()
        .map(|e| to_substrait_agg_measure(producer, e, agg.input.schema()))
        .collect::<datafusion::common::Result<Vec<_>>>()?;

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Aggregate(Box::new(AggregateRel {
            common: None,
            input: Some(input),
            grouping_expressions,
            groupings,
            measures,
            advanced_extension: None,
        }))),
    }))
}

pub fn to_substrait_agg_measure(
    producer: &mut impl SubstraitProducer,
    expr: &Expr,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Measure> {
    match expr {
        Expr::AggregateFunction(agg_fn) => from_aggregate_function(producer, agg_fn, schema),
        Expr::Alias(Alias { expr, .. }) => {
            to_substrait_agg_measure(producer, expr, schema)
        }
        _ => internal_err!(
            "Expression must be compatible with aggregation. Unsupported expression: {:?}. ExpressionType: {:?}",
            expr,
            expr.variant_name()
        ),
    }
}
