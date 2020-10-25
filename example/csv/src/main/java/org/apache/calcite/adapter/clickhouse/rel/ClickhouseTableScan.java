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

import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class ClickhouseTableScan extends TableScan implements ClickhouseRel {

  private final ClickhouseConvention convention;

  final int[] fields;

  public ClickhouseTableScan(RelOptCluster cluster, RelOptTable table,
      ClickhouseConvention convention) {
    this(cluster, convention, table, new int[0]);
  }

  public ClickhouseTableScan(RelOptCluster cluster, ClickhouseConvention convention,
      RelOptTable table, int[] fields) {
    super(cluster, cluster.traitSetOf(convention), ImmutableList.of(), table);
    this.convention = convention;
    this.fields = fields;
    assert getConvention() instanceof ClickhouseConvention;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // Multiply the cost by a factor that makes a scan more attractive if it
    // has significantly fewer fields than the original scan.
    //
    // The "+ 2D" on top and bottom keeps the function fairly smooth.
    //
    // For example, if table has 3 fields, project has 1 field,
    // then factor = (1 + 2) / (3 + 2) = 0.6
    RelOptCost cost = super.computeSelfCost(planner, mq)
        .multiplyBy(((double) fields.length + 2D)
            / ((double) table.getRowType().getFieldCount() + 2D));
    return cost;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("fields", Primitive.asList(fields));
  }

  @Override
  public RelDataType deriveRowType() {
    if (fields.length == 0) {
      return table.getRowType();
    }
    final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
    final RelDataTypeFactory.Builder builder =
        getCluster().getTypeFactory().builder();
    // Only add selected fields
    for (int field : fields) {
      builder.add(fieldList.get(field));
    }
    return builder.build();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return super.copy(traitSet, inputs);
  }
}
