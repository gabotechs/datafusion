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

use crate::logical_plan::producer::r#type::to_substrait_type;
use crate::logical_plan::producer::substrait_producer::SubstraitProducer;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::TryCast;
use substrait::proto::expression::cast::FailureBehavior;
use substrait::proto::expression::RexType;
use substrait::proto::Expression;

pub fn from_try_cast(
    producer: &mut impl SubstraitProducer,
    cast: &TryCast,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let TryCast { expr, data_type } = cast;
    Ok(Expression {
        rex_type: Some(RexType::Cast(Box::new(
            substrait::proto::expression::Cast {
                r#type: Some(to_substrait_type(data_type, true)?),
                input: Some(Box::new(producer.handle_expr(expr, schema)?)),
                failure_behavior: FailureBehavior::ReturnNull.into(),
            },
        ))),
    })
}
