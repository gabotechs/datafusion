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

use crate::logical_plan::producer::bound::to_substrait_bound;
use crate::logical_plan::producer::substrait_producer::SubstraitProducer;
use crate::logical_plan::producer::utils::substrait_sort_field;
use datafusion::common::{not_impl_err, DFSchemaRef};
use datafusion::logical_expr::expr::{WindowFunction, WindowFunctionParams};
use datafusion::logical_expr::{WindowFrame, WindowFrameUnits};
use substrait::proto::expression::window_function::{Bound, BoundsType};
use substrait::proto::expression::RexType;
use substrait::proto::expression::WindowFunction as SubstraitWindowFunction;
use substrait::proto::function_argument::ArgType;
use substrait::proto::{Expression, FunctionArgument, SortField};

pub fn from_window_function(
    producer: &mut impl SubstraitProducer,
    window_fn: &WindowFunction,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let WindowFunction {
        fun,
        params:
            WindowFunctionParams {
                args,
                partition_by,
                order_by,
                window_frame,
                null_treatment: _,
            },
    } = window_fn;
    // function reference
    let function_anchor = producer.register_function(fun.to_string());
    // arguments
    let mut arguments: Vec<FunctionArgument> = vec![];
    for arg in args {
        arguments.push(FunctionArgument {
            arg_type: Some(ArgType::Value(producer.handle_expr(arg, schema)?)),
        });
    }
    // partition by expressions
    let partition_by = partition_by
        .iter()
        .map(|e| producer.handle_expr(e, schema))
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    // order by expressions
    let order_by = order_by
        .iter()
        .map(|e| substrait_sort_field(producer, e, schema))
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    // window frame
    let bounds = to_substrait_bounds(window_frame)?;
    let bound_type = to_substrait_bound_type(window_frame)?;
    Ok(make_substrait_window_function(
        function_anchor,
        arguments,
        partition_by,
        order_by,
        bounds,
        bound_type,
    ))
}

fn to_substrait_bounds(
    window_frame: &WindowFrame,
) -> datafusion::common::Result<(Bound, Bound)> {
    Ok((
        to_substrait_bound(&window_frame.start_bound),
        to_substrait_bound(&window_frame.end_bound),
    ))
}

fn to_substrait_bound_type(
    window_frame: &WindowFrame,
) -> datafusion::common::Result<BoundsType> {
    match window_frame.units {
        WindowFrameUnits::Rows => Ok(BoundsType::Rows), // ROWS
        WindowFrameUnits::Range => Ok(BoundsType::Range), // RANGE
        // TODO: Support GROUPS
        unit => not_impl_err!("Unsupported window frame unit: {unit:?}"),
    }
}

fn make_substrait_window_function(
    function_reference: u32,
    arguments: Vec<FunctionArgument>,
    partitions: Vec<Expression>,
    sorts: Vec<SortField>,
    bounds: (Bound, Bound),
    bounds_type: BoundsType,
) -> Expression {
    #[allow(deprecated)]
    Expression {
        rex_type: Some(RexType::WindowFunction(SubstraitWindowFunction {
            function_reference,
            arguments,
            partitions,
            sorts,
            options: vec![],
            output_type: None,
            phase: 0,      // default to AGGREGATION_PHASE_UNSPECIFIED
            invocation: 0, // TODO: fix
            lower_bound: Some(bounds.0),
            upper_bound: Some(bounds.1),
            args: vec![],
            bounds_type: bounds_type as i32,
        })),
    }
}
