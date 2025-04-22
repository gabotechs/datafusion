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
use datafusion::common::{not_impl_err, DFSchemaRef};
use datafusion::logical_expr::Expr;
use substrait::proto::expression::{RexType, ScalarFunction};
use substrait::proto::function_argument::ArgType;
use substrait::proto::{Expression, FunctionArgument};

pub fn from_unary_expr(
    producer: &mut impl SubstraitProducer,
    expr: &Expr,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let (fn_name, arg) = match expr {
        Expr::Not(arg) => ("not", arg),
        Expr::IsNull(arg) => ("is_null", arg),
        Expr::IsNotNull(arg) => ("is_not_null", arg),
        Expr::IsTrue(arg) => ("is_true", arg),
        Expr::IsFalse(arg) => ("is_false", arg),
        Expr::IsUnknown(arg) => ("is_unknown", arg),
        Expr::IsNotTrue(arg) => ("is_not_true", arg),
        Expr::IsNotFalse(arg) => ("is_not_false", arg),
        Expr::IsNotUnknown(arg) => ("is_not_unknown", arg),
        Expr::Negative(arg) => ("negate", arg),
        expr => not_impl_err!("Unsupported expression: {expr:?}")?,
    };
    to_substrait_unary_scalar_fn(producer, fn_name, arg, schema)
}

/// Util to generate substrait [RexType::ScalarFunction] with one argument
fn to_substrait_unary_scalar_fn(
    producer: &mut impl SubstraitProducer,
    fn_name: &str,
    arg: &Expr,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let function_anchor = producer.register_function(fn_name.to_string());
    let substrait_expr = producer.handle_expr(arg, schema)?;

    Ok(Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments: vec![FunctionArgument {
                arg_type: Some(ArgType::Value(substrait_expr)),
            }],
            output_type: None,
            options: vec![],
            ..Default::default()
        })),
    })
}
