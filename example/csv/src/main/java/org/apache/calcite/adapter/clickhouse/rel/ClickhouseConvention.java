/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.adapter.clickhouse.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;

public class ClickhouseConvention extends Convention.Impl {

  public ClickhouseConvention() {
    super("Clickhouse", ClickhouseRel.class);
  }

  @Override
  public void register(RelOptPlanner planner) {
    for (RelOptRule rule : ClickhouseRules.rules(this)) {
      planner.addRule(rule);
    }
    planner.addRule(CoreRules.FILTER_SET_OP_TRANSPOSE);
    planner.addRule(CoreRules.PROJECT_REMOVE);
  }
}
