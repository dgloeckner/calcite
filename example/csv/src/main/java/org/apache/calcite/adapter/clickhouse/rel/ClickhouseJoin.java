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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;

import java.util.Set;

// TODO: push down fields past join
public class ClickhouseJoin extends Join implements ClickhouseRel {

  public ClickhouseJoin(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    assert getConvention() instanceof ClickhouseConvention;
    assert getConvention() == left.getConvention();
    assert getConvention() == right.getConvention();
  }

  @Override
  public ClickhouseJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
      RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new ClickhouseJoin(getCluster(), getTraitSet(), left, right, conditionExpr,
        variablesSet, joinType);
  }
}
