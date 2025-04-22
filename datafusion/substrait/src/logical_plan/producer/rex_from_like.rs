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
use crate::logical_plan::producer::utils::to_substrait_literal_expr;
use datafusion::common::{DFSchemaRef, ScalarValue};
use datafusion::logical_expr::{Expr, Like};
use substrait::proto::expression::{RexType, ScalarFunction};
use substrait::proto::function_argument::ArgType;
use substrait::proto::{Expression, FunctionArgument};

pub fn from_like(
    producer: &mut impl SubstraitProducer,
    like: &Like,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let Like {
        negated,
        expr,
        pattern,
        escape_char,
        case_insensitive,
    } = like;
    make_substrait_like_expr(
        producer,
        *case_insensitive,
        *negated,
        expr,
        pattern,
        *escape_char,
        schema,
    )
}

fn make_substrait_like_expr(
    producer: &mut impl SubstraitProducer,
    ignore_case: bool,
    negated: bool,
    expr: &Expr,
    pattern: &Expr,
    escape_char: Option<char>,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let function_anchor = if ignore_case {
        producer.register_function("ilike".to_string())
    } else {
        producer.register_function("like".to_string())
    };
    let expr = producer.handle_expr(expr, schema)?;
    let pattern = producer.handle_expr(pattern, schema)?;
    let escape_char = to_substrait_literal_expr(
        producer,
        &ScalarValue::Utf8(escape_char.map(|c| c.to_string())),
    )?;
    let arguments = vec![
        FunctionArgument {
            arg_type: Some(ArgType::Value(expr)),
        },
        FunctionArgument {
            arg_type: Some(ArgType::Value(pattern)),
        },
        FunctionArgument {
            arg_type: Some(ArgType::Value(escape_char)),
        },
    ];

    #[allow(deprecated)]
    let substrait_like = Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments,
            output_type: None,
            args: vec![],
            options: vec![],
        })),
    };

    if negated {
        let function_anchor = producer.register_function("not".to_string());

        #[allow(deprecated)]
        Ok(Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: function_anchor,
                arguments: vec![FunctionArgument {
                    arg_type: Some(ArgType::Value(substrait_like)),
                }],
                output_type: None,
                args: vec![],
                options: vec![],
            })),
        })
    } else {
        Ok(substrait_like)
    }
}
