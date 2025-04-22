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
use crate::logical_plan::producer::utils::create_project_remapping;
use datafusion::logical_expr::Projection;
use substrait::proto::rel::RelType;
use substrait::proto::{ProjectRel, Rel, RelCommon};

pub fn from_projection(
    producer: &mut impl SubstraitProducer,
    p: &Projection,
) -> datafusion::common::Result<Box<Rel>> {
    let expressions = p
        .expr
        .iter()
        .map(|e| producer.handle_expr(e, p.input.schema()))
        .collect::<datafusion::common::Result<Vec<_>>>()?;

    let emit_kind = create_project_remapping(
        expressions.len(),
        p.input.as_ref().schema().fields().len(),
    );
    let common = RelCommon {
        emit_kind: Some(emit_kind),
        hint: None,
        advanced_extension: None,
    };

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Project(Box::new(ProjectRel {
            common: Some(common),
            input: Some(producer.handle_plan(p.input.as_ref())?),
            expressions,
            advanced_extension: None,
        }))),
    }))
}
