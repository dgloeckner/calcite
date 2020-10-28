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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.RelSubset;
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
          b0.operand(Project.class).oneInput(b1 ->
              b1.operand(ClickhousePivot.class).noInputs()))
      .withDescription("Push project into table scan")
      .as(ClickhouseProjectPushdownRule.Config.class)
      .toRule();

  public static final ClickhouseVerticalFilterPushDownRule VERTICAL_FILTER_PUSH_DOWN_RULE =
      RelRule.Config.EMPTY
      .withOperandSupplier(b0 ->
          b0.operand(ClickhouseFilter.class).oneInput(b1 ->
              b1.operand(ClickhousePivot.class).noInputs()))
      .withDescription("Push vertical filter past pivot")
      .as(ClickhouseVerticalFilterPushDownRule.Config.class)
      .toRule();

  public static Collection<RelOptRule> rules(ClickhouseConvention convention) {
    List<RelOptRule> rules = new ArrayList<>();
    //rules.add(ClickhouseJoinConverterRule.create(convention));
    rules.add(ClickhouseFilterConverterRule.create(convention));
    rules.add(ClickhouseProjectConverterRule.create(convention));
    rules.add(ClickhouseAggregateConverterRule.create(convention));
    rules.add(ClickhouseToEnumerableConverterRule.create(convention));
    rules.add(VERTICAL_FILTER_PUSH_DOWN_RULE);
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

  public static class ClickhouseToEnumerableConverterRule extends ClickhouseAggregateConverterRule {

    /**
     * Creates a ClickhouseToEnumerableConverterRule.
     */
    public static ClickhouseToEnumerableConverterRule create(ClickhouseConvention in) {
      return Config.INSTANCE
          .withConversion(RelNode.class, in, EnumerableConvention.INSTANCE,
              "ClickhouseToEnumerableConverterRule")
          .withRuleFactory(ClickhouseToEnumerableConverterRule::new)
          .toRule(ClickhouseToEnumerableConverterRule.class);
    }

    protected ClickhouseToEnumerableConverterRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
      return new ClickhouseToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
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

    @Override
    public boolean matches(RelOptRuleCall call) {
      LogicalProject project = call.rel(0);
      for (RexNode e : project.getProjects()) {
        if (!(e instanceof RexInputRef)) {
          return false;
        }
      }
      return super.matches(call);
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
      final Project project = call.rel(0);
      final ClickhousePivot scan = call.rel(1);
      int[] fields = getProjectFields(project.getProjects());
      if (fields == null) {
        // Project contains expressions more complex than just field references.
        return;
      }
      call.transformTo(
          new ClickhousePivot(
              scan.getCluster(),
              (ClickhouseConvention) scan.getConvention(),
              scan.getTable(),
              scan.getClickhouseTable(), fields));
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

  // Pushes a vertical filter expression below the pivot element
  public static class ClickhouseVerticalFilterPushDownRule extends RelRule<ClickhouseVerticalFilterPushDownRule.Config> {

    public ClickhouseVerticalFilterPushDownRule(Config config) {
      super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      Filter filter = call.rel(0);
      ClickhousePivot pivot = findPivot(filter);
      return pivot != null;
    }

    private ClickhousePivot findPivot(RelNode current) {
      List<RelNode> children = current.getInputs();
      for (RelNode child : children) {
        if (child instanceof ClickhousePivot) {
          return (ClickhousePivot) child;
        }
        if (child instanceof Join) {
          // Cannot push past joins etc.
          return null;
        }
      }
      for (RelNode child : children) {
        ClickhousePivot found = findPivot(child);
        if (found != null) {
          return found;
        }
      }
      return null;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      RelNode inputCopy = filter.getInput().copy(filter.getInput().getTraitSet(),
          filter.getInput().getInputs());
      ClickhousePivot pivot = findPivot(inputCopy);
      ClickhouseMaterialize materialize = new ClickhouseMaterialize(filter.getCluster(),
          pivot.getTraitSet());
      pivot.addInput(materialize);
      call.transformTo(inputCopy);
    }

    /**
     * Rule configuration.
     */
    public interface Config extends RelRule.Config {

      @Override
      default ClickhouseVerticalFilterPushDownRule toRule() {
        return new ClickhouseVerticalFilterPushDownRule(this);
      }
    }

  }

}
