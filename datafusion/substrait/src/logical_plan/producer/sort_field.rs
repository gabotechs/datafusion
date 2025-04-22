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
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::expr;
use substrait::proto::sort_field::{SortDirection, SortKind};
use substrait::proto::SortField;

/// Converts sort expression to corresponding substrait `SortField`
pub(super) fn to_substrait_sort_field(
    producer: &mut impl SubstraitProducer,
    sort: &expr::Sort,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<SortField> {
    let sort_kind = match (sort.asc, sort.nulls_first) {
        (true, true) => SortDirection::AscNullsFirst,
        (true, false) => SortDirection::AscNullsLast,
        (false, true) => SortDirection::DescNullsFirst,
        (false, false) => SortDirection::DescNullsLast,
    };
    Ok(SortField {
        expr: Some(producer.handle_expr(&sort.expr, schema)?),
        sort_kind: Some(SortKind::Direction(sort_kind.into())),
    })
}
