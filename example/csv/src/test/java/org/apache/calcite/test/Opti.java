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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.util.ArrayList;
import java.util.List;

public class Opti {

  public RelRoot optimize(RelRoot root, DataContext dataContext) {
    final RelOptPlanner planner = root.rel.getCluster().getPlanner();
    ((VolcanoPlanner) planner).setNoneConventionHasInfiniteCost(false); // FIXME: not sure why this is needed
    planner.addRule(TestRule.Config.DEFAULT.toRule());

    planner.setExecutor(new RexExecutorImpl(dataContext));

    final List<RelOptMaterialization> materializationList = new ArrayList<>();
    final List<RelOptLattice> latticeList = new ArrayList<>();

    final RelTraitSet desiredTraits = RelTraitSet.createEmpty();
    desiredTraits.plus(EnumerableConvention.INSTANCE);

    final Program program = Programs.standard();
    final RelNode rootRel4 = program.run(
        planner, root.rel, desiredTraits, materializationList, latticeList);
    return root.withRel(rootRel4);
  }
}
