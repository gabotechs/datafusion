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

use datafusion::common::ScalarValue;
use datafusion::logical_expr::WindowFrameBound;
use substrait::proto::expression::{
    window_function::bound as SubstraitBound, window_function::bound::Kind as BoundKind,
    window_function::Bound,
};

pub(super) fn to_substrait_bound(bound: &WindowFrameBound) -> Bound {
    match bound {
        WindowFrameBound::CurrentRow => Bound {
            kind: Some(BoundKind::CurrentRow(SubstraitBound::CurrentRow {})),
        },
        WindowFrameBound::Preceding(s) => match to_substrait_bound_offset(s) {
            Some(offset) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding { offset })),
            },
            None => Bound {
                kind: Some(BoundKind::Unbounded(SubstraitBound::Unbounded {})),
            },
        },
        WindowFrameBound::Following(s) => match to_substrait_bound_offset(s) {
            Some(offset) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following { offset })),
            },
            None => Bound {
                kind: Some(BoundKind::Unbounded(SubstraitBound::Unbounded {})),
            },
        },
    }
}

fn to_substrait_bound_offset(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::UInt8(Some(v)) => Some(*v as i64),
        ScalarValue::UInt16(Some(v)) => Some(*v as i64),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        ScalarValue::UInt64(Some(v)) => Some(*v as i64),
        ScalarValue::Int8(Some(v)) => Some(*v as i64),
        ScalarValue::Int16(Some(v)) => Some(*v as i64),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int64(Some(v)) => Some(*v),
        _ => None,
    }
}
