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

package org.apache.calcite.test;


import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;

import java.util.Collections;

// Rule just to show that rules get fired ;)
public class TestRule
    extends RelRule<TestRule.Config> {

  protected TestRule(TestRule.Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);
    final LogicalTableScan scan = call.rel(1);
    RelOptTable otherTable =
        scan.getTable().getRelOptSchema().getTableForMember(Collections.singletonList("REPLACEMENT"));
    call.transformTo(new LogicalTableScan(scan.getCluster(), scan.getTraitSet(), scan.getHints(),
        otherTable));
  }

  /**
   * Rule configuration.
   */
  public interface Config extends RelRule.Config {
    TestRule.Config DEFAULT = EMPTY
        .withOperandSupplier(b0 ->
            b0.operand(LogicalProject.class).oneInput(b1 ->
                b1.operand(LogicalTableScan.class).noInputs()))
        .as(TestRule.Config.class);

    @Override
    default TestRule toRule() {
      return new TestRule(this);
    }
  }
}
