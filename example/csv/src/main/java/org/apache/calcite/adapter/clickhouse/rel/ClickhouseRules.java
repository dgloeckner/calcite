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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ClickhouseRules {

  private static final ClickhouseProjectPushdownRule PROJECT_PUSHDOWN = RelRule.Config.EMPTY
      .withOperandSupplier(b0 ->
          b0.operand(LogicalProject.class).oneInput(b1 ->
              b1.operand(ClickhouseTableScan.class).noInputs()))
      .withDescription("Push project into table scan")
      .as(ClickhouseProjectPushdownRule.Config.class)
      .toRule();

  public static Collection<RelOptRule> rules(ClickhouseConvention convention) {
    List<RelOptRule> rules = new ArrayList<>();
    rules.add(ClickhouseJoinConverterRule.create(convention));
    rules.add(ClickhouseFilterConverterRule.create(convention));
    rules.add(ClickhouseProjectConverterRule.create(convention));
    rules.add(ClickhouseAggregateConverterRule.create(convention));
    rules.add(PROJECT_PUSHDOWN);
    return rules;
  }

  /**
   * Abstract base class for rule that converts to Clickhouse convention.
   */
  abstract static class ClickhouseConverterRule extends ConverterRule {
    protected ClickhouseConverterRule(Config config) {
      super(config);
    }
  }

  /**
   * Rule that converts a join to clickhouse join.
   */
  public static class ClickhouseJoinConverterRule extends ClickhouseConverterRule {
    /**
     * Creates a ClickhouseJoinRule.
     */
    public static ClickhouseJoinConverterRule create(ClickhouseConvention out) {
      return Config.INSTANCE
          .withConversion(Join.class, Convention.NONE, out, "ClickhouseJoinRule")
          .withRuleFactory(ClickhouseJoinConverterRule::new)
          .toRule(ClickhouseJoinConverterRule.class);
    }

    /**
     * Called from the Config.
     */
    protected ClickhouseJoinConverterRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      final Join join = (Join) rel;
      final List<RelNode> newInputs = new ArrayList<>();
      for (RelNode input : join.getInputs()) {
        if (input.getConvention() != getOutTrait()) {
          input =
              convert(input,
                  input.getTraitSet().replace(out));
        }
        newInputs.add(input);
      }
      switch (join.getJoinType()) {
      case SEMI:
      case ANTI:
      default:
        return new ClickhouseJoin(join.getCluster(), rel.getTraitSet().replace(out),
            newInputs.get(0), newInputs.get(1),
            join.getCondition(), join.getVariablesSet(), join.getJoinType());
      }
    }
  }

  /**
   * Rule that converts a project to clickhouse project.
   */
  public static class ClickhouseProjectConverterRule extends ClickhouseConverterRule {

    public static ClickhouseProjectConverterRule create(ClickhouseConvention out) {
      return Config.INSTANCE
          .withConversion(Project.class, Convention.NONE, out, "ClickhouseProjectConverterRule")
          .withRuleFactory(ClickhouseProjectConverterRule::new)
          .toRule(ClickhouseProjectConverterRule.class);
    }

    /**
     * Called from the Config.
     */
    protected ClickhouseProjectConverterRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      final Project project = (Project) rel;
      return new ClickhouseProject(project.getCluster(),
          project.getTraitSet().replace(out),
          convert(project.getInput(), out),
          project.getProjects(), project.getRowType());
    }
  }


  /**
   * Rule that converts a filter to clickhouse filter.
   */
  public static class ClickhouseFilterConverterRule extends ClickhouseConverterRule {

    public static ClickhouseFilterConverterRule create(ClickhouseConvention out) {
      return Config.INSTANCE
          .withConversion(Filter.class, Convention.NONE, out,
              "ClickhouseFilterConverterRule")
          .withRuleFactory(ClickhouseFilterConverterRule::new)
          .toRule(ClickhouseFilterConverterRule.class);
    }

    /**
     * Called from the Config.
     */
    protected ClickhouseFilterConverterRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      final Filter filter = (Filter) rel;
      return new ClickhouseFilter(filter.getCluster(),
          filter.getTraitSet().replace(out),
          convert(filter.getInput(), out),
          filter.getCondition());
    }
  }

  /**
   * Rule that converts a aggregate to clickhouse aggregate.
   */
  public static class ClickhouseAggregateConverterRule extends ClickhouseConverterRule {

    public static ClickhouseAggregateConverterRule create(ClickhouseConvention out) {
      return Config.INSTANCE
          .withConversion(Aggregate.class, Convention.NONE, out,
              "ClickhouseAggregateConverterRule")
          .withRuleFactory(ClickhouseAggregateConverterRule::new)
          .toRule(ClickhouseAggregateConverterRule.class);
    }

    /**
     * Called from the Config.
     */
    protected ClickhouseAggregateConverterRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      final Aggregate aggregate = (Aggregate) rel;
      return new ClickhouseAggregate(aggregate.getCluster(),
          aggregate.getTraitSet().replace(out),
          convert(aggregate.getInput(), out),
          aggregate.getGroupSet(),
          aggregate.getGroupSets(),
          aggregate.getAggCallList());
    }
  }

  // Pushes projected fields into tabla scan
  public static class ClickhouseProjectPushdownRule extends RelRule<ClickhouseProjectPushdownRule.Config> {

    public ClickhouseProjectPushdownRule(Config config) {
      super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      return super.matches(call);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final LogicalProject project = call.rel(0);
      final ClickhouseTableScan scan = call.rel(1);
      int[] fields = getProjectFields(project.getProjects());
      if (fields == null) {
        // Project contains expressions more complex than just field references.
        return;
      }
      call.transformTo(
          new ClickhouseTableScan(
              scan.getCluster(),
              (ClickhouseConvention) scan.getConvention(),
              scan.getTable(),
              new int[]{1}));
    }

    private int[] getProjectFields(List<RexNode> exps) {
      final int[] fields = new int[exps.size()];
      for (int i = 0; i < exps.size(); i++) {
        final RexNode exp = exps.get(i);
        if (exp instanceof RexInputRef) {
          fields[i] = ((RexInputRef) exp).getIndex();
        } else {
          return null; // not a simple projection
        }
      }
      return fields;
    }

    /**
     * Rule configuration.
     */
    public interface Config extends RelRule.Config {

      @Override
      default ClickhouseProjectPushdownRule toRule() {
        return new ClickhouseProjectPushdownRule(this);
      }
    }

  }


  // Pushes filter expression into table scan
  public static class ClickhouseFilterPushdownRule extends RelRule<ClickhouseFilterPushdownRule.Config> {

    public ClickhouseFilterPushdownRule(Config config) {
      super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      return super.matches(call);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final LogicalProject project = call.rel(0);
      final ClickhouseTableScan scan = call.rel(1);
      int[] fields = getProjectFields(project.getProjects());
      if (fields == null) {
        // Project contains expressions more complex than just field references.
        return;
      }
      call.transformTo(
          new ClickhouseTableScan(
              scan.getCluster(),
              (ClickhouseConvention) scan.getConvention(),
              scan.getTable(),
              new int[]{1}));
    }

    private int[] getProjectFields(List<RexNode> exps) {
      final int[] fields = new int[exps.size()];
      for (int i = 0; i < exps.size(); i++) {
        final RexNode exp = exps.get(i);
        if (exp instanceof RexInputRef) {
          fields[i] = ((RexInputRef) exp).getIndex();
        } else {
          return null; // not a simple projection
        }
      }
      return fields;
    }

    /**
     * Rule configuration.
     */
    public interface Config extends RelRule.Config {

      @Override
      default ClickhouseFilterPushdownRule toRule() {
        return new ClickhouseFilterPushdownRule(this);
      }
    }

  }

}
