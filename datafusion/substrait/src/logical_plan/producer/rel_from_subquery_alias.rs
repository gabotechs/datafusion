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
use datafusion::logical_expr::SubqueryAlias;
use substrait::proto::Rel;

pub fn from_subquery_alias(
    producer: &mut impl SubstraitProducer,
    alias: &SubqueryAlias,
) -> datafusion::common::Result<Box<Rel>> {
    // Do nothing if encounters SubqueryAlias
    // since there is no corresponding relation type in Substrait
    producer.handle_plan(alias.input.as_ref())
}
