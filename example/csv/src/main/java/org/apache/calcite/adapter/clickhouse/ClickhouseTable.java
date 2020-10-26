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

package org.apache.calcite.adapter.clickhouse;

import org.apache.calcite.adapter.clickhouse.rel.ClickhouseTableScan;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;

public class ClickhouseTable extends AbstractTable implements TranslatableTable {

  private final ClickhouseSchema schema;

  public ClickhouseTable(ClickhouseSchema schema) {
    this.schema = schema;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    JavaTypeFactory jtf = (JavaTypeFactory) typeFactory;
    return jtf.createStructType(
        Arrays.asList(jtf.createSqlType(SqlTypeName.CHAR),
            jtf.createSqlType(SqlTypeName.CHAR), jtf.createSqlType(SqlTypeName.CHAR),
            jtf.createSqlType(SqlTypeName.CHAR), jtf.createSqlType(SqlTypeName.INTEGER),
            jtf.createSqlType(SqlTypeName.INTEGER),
            jtf.createSqlType(SqlTypeName.INTEGER)),
        Arrays.asList("ID", "STRING1", "STRING2", "STRING3", "INT1", "INT2", "INT3"));
  }

  @Override
  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return new ClickhouseTableScan(context.getCluster(), relOptTable, schema.getConvention());
  }
}
