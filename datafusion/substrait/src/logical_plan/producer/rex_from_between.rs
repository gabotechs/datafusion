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
use datafusion::logical_expr::{Between, Operator};
use substrait::proto::Expression;

pub fn from_between(
    producer: &mut impl SubstraitProducer,
    between: &Between,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let Between {
        expr,
        negated,
        low,
        high,
    } = between;
    if *negated {
        // `expr NOT BETWEEN low AND high` can be translated into (expr < low OR high < expr)
        let substrait_expr = producer.handle_expr(expr.as_ref(), schema)?;
        let substrait_low = producer.handle_expr(low.as_ref(), schema)?;
        let substrait_high = producer.handle_expr(high.as_ref(), schema)?;

        let l_expr = make_binary_op_scalar_func(
            producer,
            &substrait_expr,
            &substrait_low,
            Operator::Lt,
        );
        let r_expr = make_binary_op_scalar_func(
            producer,
            &substrait_high,
            &substrait_expr,
            Operator::Lt,
        );

        Ok(make_binary_op_scalar_func(
            producer,
            &l_expr,
            &r_expr,
            Operator::Or,
        ))
    } else {
        // `expr BETWEEN low AND high` can be translated into (low <= expr AND expr <= high)
        let substrait_expr = producer.handle_expr(expr.as_ref(), schema)?;
        let substrait_low = producer.handle_expr(low.as_ref(), schema)?;
        let substrait_high = producer.handle_expr(high.as_ref(), schema)?;

        let l_expr = make_binary_op_scalar_func(
            producer,
            &substrait_low,
            &substrait_expr,
            Operator::LtEq,
        );
        let r_expr = make_binary_op_scalar_func(
            producer,
            &substrait_expr,
            &substrait_high,
            Operator::LtEq,
        );

        Ok(make_binary_op_scalar_func(
            producer,
            &l_expr,
            &r_expr,
            Operator::And,
        ))
    }
}
