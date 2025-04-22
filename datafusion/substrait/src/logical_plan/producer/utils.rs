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

use crate::logical_plan::producer::literal::to_substrait_literal;
use crate::logical_plan::producer::substrait_producer::SubstraitProducer;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{plan_err, DFSchemaRef, ScalarValue};
use datafusion::logical_expr::{Operator, SortExpr};
use substrait::proto::expression::{RexType, ScalarFunction};
use substrait::proto::function_argument::ArgType;
use substrait::proto::rel_common::EmitKind;
use substrait::proto::rel_common::EmitKind::Emit;
use substrait::proto::sort_field::{SortDirection, SortKind};
use substrait::proto::{rel_common, Expression, FunctionArgument, SortField};

/// By default, a Substrait Project outputs all input fields followed by all expressions.
/// A DataFusion Projection only outputs expressions. In order to keep the Substrait
/// plan consistent with DataFusion, we must apply an output mapping that skips the input
/// fields so that the Substrait Project will only output the expression fields.
pub(super) fn create_project_remapping(
    expr_count: usize,
    input_field_count: usize,
) -> EmitKind {
    let expression_field_start = input_field_count;
    let expression_field_end = expression_field_start + expr_count;
    let output_mapping = (expression_field_start..expression_field_end)
        .map(|i| i as i32)
        .collect();
    Emit(rel_common::Emit { output_mapping })
}

// Substrait wants a list of all field names, including nested fields from structs,
// also from within e.g. lists and maps. However, it does not want the list and map field names
// themselves - only proper structs fields are considered to have useful names.
pub(super) fn flatten_names(
    field: &Field,
    skip_self: bool,
    names: &mut Vec<String>,
) -> datafusion::common::Result<()> {
    if !skip_self {
        names.push(field.name().to_string());
    }
    match field.data_type() {
        DataType::Struct(fields) => {
            for field in fields {
                flatten_names(field, false, names)?;
            }
            Ok(())
        }
        DataType::List(l) => flatten_names(l, true, names),
        DataType::LargeList(l) => flatten_names(l, true, names),
        DataType::Map(m, _) => match m.data_type() {
            DataType::Struct(key_and_value) if key_and_value.len() == 2 => {
                flatten_names(&key_and_value[0], true, names)?;
                flatten_names(&key_and_value[1], true, names)
            }
            _ => plan_err!("Map fields must contain a Struct with exactly 2 fields"),
        },
        _ => Ok(()),
    }?;
    Ok(())
}

pub(super) fn to_substrait_literal_expr(
    producer: &mut impl SubstraitProducer,
    value: &ScalarValue,
) -> datafusion::common::Result<Expression> {
    let literal = to_substrait_literal(producer, value)?;
    Ok(Expression {
        rex_type: Some(RexType::Literal(literal)),
    })
}

pub(super) fn substrait_sort_field(
    producer: &mut impl SubstraitProducer,
    sort: &SortExpr,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<SortField> {
    let SortExpr {
        expr,
        asc,
        nulls_first,
    } = sort;
    let e = producer.handle_expr(expr, schema)?;
    let d = match (asc, nulls_first) {
        (true, true) => SortDirection::AscNullsFirst,
        (true, false) => SortDirection::AscNullsLast,
        (false, true) => SortDirection::DescNullsFirst,
        (false, false) => SortDirection::DescNullsLast,
    };
    Ok(SortField {
        expr: Some(e),
        sort_kind: Some(SortKind::Direction(d as i32)),
    })
}

pub fn operator_to_name(op: Operator) -> &'static str {
    match op {
        Operator::Eq => "equal",
        Operator::NotEq => "not_equal",
        Operator::Lt => "lt",
        Operator::LtEq => "lte",
        Operator::Gt => "gt",
        Operator::GtEq => "gte",
        Operator::Plus => "add",
        Operator::Minus => "subtract",
        Operator::Multiply => "multiply",
        Operator::Divide => "divide",
        Operator::Modulo => "modulus",
        Operator::And => "and",
        Operator::Or => "or",
        Operator::IsDistinctFrom => "is_distinct_from",
        Operator::IsNotDistinctFrom => "is_not_distinct_from",
        Operator::RegexMatch => "regex_match",
        Operator::RegexIMatch => "regex_imatch",
        Operator::RegexNotMatch => "regex_not_match",
        Operator::RegexNotIMatch => "regex_not_imatch",
        Operator::LikeMatch => "like_match",
        Operator::ILikeMatch => "like_imatch",
        Operator::NotLikeMatch => "like_not_match",
        Operator::NotILikeMatch => "like_not_imatch",
        Operator::BitwiseAnd => "bitwise_and",
        Operator::BitwiseOr => "bitwise_or",
        Operator::StringConcat => "str_concat",
        Operator::AtArrow => "at_arrow",
        Operator::ArrowAt => "arrow_at",
        Operator::Arrow => "arrow",
        Operator::LongArrow => "long_arrow",
        Operator::HashArrow => "hash_arrow",
        Operator::HashLongArrow => "hash_long_arrow",
        Operator::AtAt => "at_at",
        Operator::IntegerDivide => "integer_divide",
        Operator::HashMinus => "hash_minus",
        Operator::AtQuestion => "at_question",
        Operator::Question => "question",
        Operator::QuestionAnd => "question_and",
        Operator::QuestionPipe => "question_pipe",
        Operator::BitwiseXor => "bitwise_xor",
        Operator::BitwiseShiftRight => "bitwise_shift_right",
        Operator::BitwiseShiftLeft => "bitwise_shift_left",
    }
}

/// Return Substrait scalar function with two arguments
pub fn make_binary_op_scalar_func(
    producer: &mut impl SubstraitProducer,
    lhs: &Expression,
    rhs: &Expression,
    op: Operator,
) -> Expression {
    let function_anchor = producer.register_function(operator_to_name(op).to_string());
    #[allow(deprecated)]
    Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments: vec![
                FunctionArgument {
                    arg_type: Some(ArgType::Value(lhs.clone())),
                },
                FunctionArgument {
                    arg_type: Some(ArgType::Value(rhs.clone())),
                },
            ],
            output_type: None,
            args: vec![],
            options: vec![],
        })),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::extensions::Extensions;
    use crate::logical_plan::consumer::DefaultSubstraitConsumer;
    use datafusion::execution::SessionState;
    use datafusion::prelude::SessionContext;
    use std::sync::LazyLock;

    pub(crate) static TEST_SESSION_STATE: LazyLock<SessionState> =
        LazyLock::new(|| SessionContext::default().state());
    pub(crate) static TEST_EXTENSIONS: LazyLock<Extensions> =
        LazyLock::new(Extensions::default);
    pub(crate) fn test_consumer() -> DefaultSubstraitConsumer<'static> {
        let extensions = &TEST_EXTENSIONS;
        let state = &TEST_SESSION_STATE;
        DefaultSubstraitConsumer::new(extensions, state)
    }
}
