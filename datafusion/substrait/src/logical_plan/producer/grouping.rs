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
use datafusion::common::{DFSchemaRef, DataFusionError};
use datafusion::logical_expr::{Expr, GroupingSet};
use substrait::proto::aggregate_rel::Grouping;
use substrait::proto::Expression;

pub fn parse_flat_grouping_exprs(
    producer: &mut impl SubstraitProducer,
    exprs: &[Expr],
    schema: &DFSchemaRef,
    ref_group_exprs: &mut Vec<Expression>,
) -> datafusion::common::Result<Grouping> {
    let mut expression_references = vec![];
    let mut grouping_expressions = vec![];

    for e in exprs {
        let rex = producer.handle_expr(e, schema)?;
        grouping_expressions.push(rex.clone());
        ref_group_exprs.push(rex);
        expression_references.push((ref_group_exprs.len() - 1) as u32);
    }
    #[allow(deprecated)]
    Ok(Grouping {
        grouping_expressions,
        expression_references,
    })
}

pub fn to_substrait_groupings(
    producer: &mut impl SubstraitProducer,
    exprs: &[Expr],
    schema: &DFSchemaRef,
) -> datafusion::common::Result<(Vec<Expression>, Vec<Grouping>)> {
    let mut ref_group_exprs = vec![];
    let groupings = match exprs.len() {
        1 => match &exprs[0] {
            Expr::GroupingSet(gs) => match gs {
                GroupingSet::Cube(_) => Err(DataFusionError::NotImplemented(
                    "GroupingSet CUBE is not yet supported".to_string(),
                )),
                GroupingSet::GroupingSets(sets) => Ok(sets
                    .iter()
                    .map(|set| {
                        parse_flat_grouping_exprs(
                            producer,
                            set,
                            schema,
                            &mut ref_group_exprs,
                        )
                    })
                    .collect::<datafusion::common::Result<Vec<_>>>()?),
                GroupingSet::Rollup(set) => {
                    let mut sets: Vec<Vec<Expr>> = vec![vec![]];
                    for i in 0..set.len() {
                        sets.push(set[..=i].to_vec());
                    }
                    Ok(sets
                        .iter()
                        .rev()
                        .map(|set| {
                            parse_flat_grouping_exprs(
                                producer,
                                set,
                                schema,
                                &mut ref_group_exprs,
                            )
                        })
                        .collect::<datafusion::common::Result<Vec<_>>>()?)
                }
            },
            _ => Ok(vec![parse_flat_grouping_exprs(
                producer,
                exprs,
                schema,
                &mut ref_group_exprs,
            )?]),
        },
        _ => Ok(vec![parse_flat_grouping_exprs(
            producer,
            exprs,
            schema,
            &mut ref_group_exprs,
        )?]),
    }?;
    Ok((ref_group_exprs, groupings))
}
