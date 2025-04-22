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

use crate::logical_plan::producer::join_expr::to_substrait_join_expr;
use crate::logical_plan::producer::join_type::to_substrait_jointype;
use crate::logical_plan::producer::make_binary_op_scalar_func;
use crate::logical_plan::producer::substrait_producer::SubstraitProducer;
use datafusion::common::{not_impl_err, JoinConstraint};
use datafusion::logical_expr::{Join, Operator};
use std::sync::Arc;
use substrait::proto::rel::RelType;
use substrait::proto::{JoinRel, Rel};

pub fn from_join(
    producer: &mut impl SubstraitProducer,
    join: &Join,
) -> datafusion::common::Result<Box<Rel>> {
    let left = producer.handle_plan(join.left.as_ref())?;
    let right = producer.handle_plan(join.right.as_ref())?;
    let join_type = to_substrait_jointype(join.join_type);
    // we only support basic joins so return an error for anything not yet supported
    match join.join_constraint {
        JoinConstraint::On => {}
        JoinConstraint::Using => return not_impl_err!("join constraint: `using`"),
    }
    let in_join_schema = Arc::new(join.left.schema().join(join.right.schema())?);

    // convert filter if present
    let join_filter = match &join.filter {
        Some(filter) => Some(producer.handle_expr(filter, &in_join_schema)?),
        None => None,
    };

    // map the left and right columns to binary expressions in the form `l = r`
    // build a single expression for the ON condition, such as `l.a = r.a AND l.b = r.b`
    let eq_op = if join.null_equals_null {
        Operator::IsNotDistinctFrom
    } else {
        Operator::Eq
    };
    let join_on = to_substrait_join_expr(producer, &join.on, eq_op, &in_join_schema)?;

    // create conjunction between `join_on` and `join_filter` to embed all join conditions,
    // whether equal or non-equal in a single expression
    let join_expr = match &join_on {
        Some(on_expr) => match &join_filter {
            Some(filter) => Some(Box::new(make_binary_op_scalar_func(
                producer,
                on_expr,
                filter,
                Operator::And,
            ))),
            None => join_on.map(Box::new), // the join expression will only contain `join_on` if filter doesn't exist
        },
        None => match &join_filter {
            Some(_) => join_filter.map(Box::new), // the join expression will only contain `join_filter` if the `on` condition doesn't exist
            None => None,
        },
    };

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Join(Box::new(JoinRel {
            common: None,
            left: Some(left),
            right: Some(right),
            r#type: join_type as i32,
            expression: join_expr,
            post_join_filter: None,
            advanced_extension: None,
        }))),
    }))
}
