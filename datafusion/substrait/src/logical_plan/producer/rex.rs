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
use datafusion::common::{internal_err, not_impl_err, DFSchemaRef};
use datafusion::logical_expr::Expr;
use substrait::proto::Expression;

/// Convert DataFusion Expr to Substrait Rex
///
/// # Arguments
/// * `producer` - SubstraitProducer implementation which the handles the actual conversion
/// * `expr` - DataFusion expression to convert into a Substrait expression
/// * `schema` - DataFusion input schema for looking up columns
pub fn to_substrait_rex(
    producer: &mut impl SubstraitProducer,
    expr: &Expr,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    match expr {
        Expr::Alias(expr) => producer.handle_alias(expr, schema),
        Expr::Column(expr) => producer.handle_column(expr, schema),
        Expr::ScalarVariable(_, _) => {
            not_impl_err!("Cannot convert {expr:?} to Substrait")
        }
        Expr::Literal(expr) => producer.handle_literal(expr),
        Expr::BinaryExpr(expr) => producer.handle_binary_expr(expr, schema),
        Expr::Like(expr) => producer.handle_like(expr, schema),
        Expr::SimilarTo(_) => not_impl_err!("Cannot convert {expr:?} to Substrait"),
        Expr::Not(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsNotNull(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsNull(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsTrue(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsFalse(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsUnknown(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsNotTrue(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsNotFalse(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsNotUnknown(_) => producer.handle_unary_expr(expr, schema),
        Expr::Negative(_) => producer.handle_unary_expr(expr, schema),
        Expr::Between(expr) => producer.handle_between(expr, schema),
        Expr::Case(expr) => producer.handle_case(expr, schema),
        Expr::Cast(expr) => producer.handle_cast(expr, schema),
        Expr::TryCast(expr) => producer.handle_try_cast(expr, schema),
        Expr::ScalarFunction(expr) => producer.handle_scalar_function(expr, schema),
        Expr::AggregateFunction(_) => {
            internal_err!(
                "AggregateFunction should only be encountered as part of a LogicalPlan::Aggregate"
            )
        }
        Expr::WindowFunction(expr) => producer.handle_window_function(expr, schema),
        Expr::InList(expr) => producer.handle_in_list(expr, schema),
        Expr::Exists(expr) => not_impl_err!("Cannot convert {expr:?} to Substrait"),
        Expr::InSubquery(expr) => producer.handle_in_subquery(expr, schema),
        Expr::ScalarSubquery(expr) => {
            not_impl_err!("Cannot convert {expr:?} to Substrait")
        }
        #[expect(deprecated)]
        Expr::Wildcard { .. } => not_impl_err!("Cannot convert {expr:?} to Substrait"),
        Expr::GroupingSet(expr) => not_impl_err!("Cannot convert {expr:?} to Substrait"),
        Expr::Placeholder(expr) => not_impl_err!("Cannot convert {expr:?} to Substrait"),
        Expr::OuterReferenceColumn(_, _) => {
            not_impl_err!("Cannot convert {expr:?} to Substrait")
        }
        Expr::Unnest(expr) => not_impl_err!("Cannot convert {expr:?} to Substrait"),
    }
}
