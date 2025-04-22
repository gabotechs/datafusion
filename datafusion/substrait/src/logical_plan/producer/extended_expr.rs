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

use crate::logical_plan::producer::named_struct::to_substrait_named_struct;
use crate::logical_plan::producer::substrait_producer::{
    DefaultSubstraitProducer, SubstraitProducer,
};
use crate::logical_plan::producer::utils::flatten_names;
use datafusion::arrow::datatypes::Field;
use datafusion::common::DFSchemaRef;
use datafusion::execution::SessionState;
use datafusion::logical_expr::Expr;
use substrait::proto::expression_reference::ExprType;
use substrait::proto::{ExpressionReference, ExtendedExpression};
use substrait::version;

/// Serializes a collection of expressions to a Substrait ExtendedExpression message
///
/// The ExtendedExpression message is a top-level message that can be used to send
/// expressions (not plans) between systems.
///
/// Each expression is also given names for the output type.  These are provided as a
/// field and not a String (since the names may be nested, e.g. a struct).  The data
/// type and nullability of this field is redundant (those can be determined by the
/// Expr) and will be ignored.
///
/// Substrait also requires the input schema of the expressions to be included in the
/// message.  The field names of the input schema will be serialized.
pub fn to_substrait_extended_expr(
    exprs: &[(&Expr, &Field)],
    schema: &DFSchemaRef,
    state: &SessionState,
) -> datafusion::common::Result<Box<ExtendedExpression>> {
    let mut producer = DefaultSubstraitProducer::new(state);
    let substrait_exprs = exprs
        .iter()
        .map(|(expr, field)| {
            let substrait_expr = producer.handle_expr(expr, schema)?;
            let mut output_names = Vec::new();
            flatten_names(field, false, &mut output_names)?;
            Ok(ExpressionReference {
                output_names,
                expr_type: Some(ExprType::Expression(substrait_expr)),
            })
        })
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    let substrait_schema = to_substrait_named_struct(schema)?;

    let extensions = producer.get_extensions();
    Ok(Box::new(ExtendedExpression {
        advanced_extensions: None,
        expected_type_urls: vec![],
        extension_uris: vec![],
        extensions: extensions.into(),
        version: Some(version::version_with_producer("datafusion")),
        referred_expr: substrait_exprs,
        base_schema: Some(substrait_schema),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::consumer::from_substrait_extended_expr;
    use datafusion::arrow::datatypes::{DataType, Schema};
    use datafusion::common::{DFSchema, DataFusionError, ScalarValue};
    use datafusion::execution::SessionStateBuilder;

    #[tokio::test]
    async fn extended_expressions() -> datafusion::common::Result<()> {
        let state = SessionStateBuilder::default().build();

        // One expression, empty input schema
        let expr = Expr::Literal(ScalarValue::Int32(Some(42)));
        let field = Field::new("out", DataType::Int32, false);
        let empty_schema = DFSchemaRef::new(DFSchema::empty());
        let substrait =
            to_substrait_extended_expr(&[(&expr, &field)], &empty_schema, &state)?;
        let roundtrip_expr = from_substrait_extended_expr(&state, &substrait).await?;

        assert_eq!(roundtrip_expr.input_schema, empty_schema);
        assert_eq!(roundtrip_expr.exprs.len(), 1);

        let (rt_expr, rt_field) = roundtrip_expr.exprs.first().unwrap();
        assert_eq!(rt_field, &field);
        assert_eq!(rt_expr, &expr);

        // Multiple expressions, with column references
        let expr1 = Expr::Column("c0".into());
        let expr2 = Expr::Column("c1".into());
        let out1 = Field::new("out1", DataType::Int32, true);
        let out2 = Field::new("out2", DataType::Utf8, true);
        let input_schema = DFSchemaRef::new(DFSchema::try_from(Schema::new(vec![
            Field::new("c0", DataType::Int32, true),
            Field::new("c1", DataType::Utf8, true),
        ]))?);

        let substrait = to_substrait_extended_expr(
            &[(&expr1, &out1), (&expr2, &out2)],
            &input_schema,
            &state,
        )?;
        let roundtrip_expr = from_substrait_extended_expr(&state, &substrait).await?;

        assert_eq!(roundtrip_expr.input_schema, input_schema);
        assert_eq!(roundtrip_expr.exprs.len(), 2);

        let mut exprs = roundtrip_expr.exprs.into_iter();

        let (rt_expr, rt_field) = exprs.next().unwrap();
        assert_eq!(rt_field, out1);
        assert_eq!(rt_expr, expr1);

        let (rt_expr, rt_field) = exprs.next().unwrap();
        assert_eq!(rt_field, out2);
        assert_eq!(rt_expr, expr2);

        Ok(())
    }

    #[tokio::test]
    async fn invalid_extended_expression() {
        let state = SessionStateBuilder::default().build();

        // Not ok if input schema is missing field referenced by expr
        let expr = Expr::Column("missing".into());
        let field = Field::new("out", DataType::Int32, false);
        let empty_schema = DFSchemaRef::new(DFSchema::empty());

        let err = to_substrait_extended_expr(&[(&expr, &field)], &empty_schema, &state);

        assert!(matches!(err, Err(DataFusionError::SchemaError(_, _))));
    }
}
