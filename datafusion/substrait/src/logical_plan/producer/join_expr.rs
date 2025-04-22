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
use crate::logical_plan::producer::utils::make_binary_op_scalar_func;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{Expr, Operator};
use substrait::proto::Expression;

pub(super) fn to_substrait_join_expr(
    producer: &mut impl SubstraitProducer,
    join_conditions: &Vec<(Expr, Expr)>,
    eq_op: Operator,
    join_schema: &DFSchemaRef,
) -> datafusion::common::Result<Option<Expression>> {
    // Only support AND conjunction for each binary expression in join conditions
    let mut exprs: Vec<Expression> = vec![];
    for (left, right) in join_conditions {
        let l = producer.handle_expr(left, join_schema)?;
        let r = producer.handle_expr(right, join_schema)?;
        // AND with existing expression
        exprs.push(make_binary_op_scalar_func(producer, &l, &r, eq_op));
    }

    let join_expr: Option<Expression> =
        exprs.into_iter().reduce(|acc: Expression, e: Expression| {
            make_binary_op_scalar_func(producer, &acc, &e, Operator::And)
        });
    Ok(join_expr)
}
