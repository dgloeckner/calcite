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

import org.apache.calcite.adapter.clickhouse.rel.ClickhouseEnumerator;
import org.apache.calcite.adapter.clickhouse.rel.ClickhousePivot;
import org.apache.calcite.adapter.clickhouse.rel.ClickhouseTableScan;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ClickhouseTable extends AbstractQueryableTable implements TranslatableTable {

  private final ClickhouseSchema schema;
  private final String name;

  public ClickhouseTable(ClickhouseSchema schema, String name) {
    super(Object[].class);
    this.schema = schema;
    this.name = name;
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
    // return new ClickhouseTableScan(context.getCluster(), relOptTable, schema.getConvention(), this);
    return new ClickhousePivot(context.getCluster(), relOptTable, schema.getConvention(), this);
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    return new ClickhouseQueryable<>(queryProvider, schema, this, tableName);
  }

  private Enumerable<Object> clickhouseQuery(List<Map.Entry<String, Class>> fields,
      List<Map.Entry<String, String>> selectFields,
      List<String> predicates, List<String> order,
      Integer offset, Integer fetch) {

    return new AbstractEnumerable<Object>() {
      @Override public Enumerator<Object> enumerator() {
        // TODO final ResultSet results = session.execute(query);
        return new ClickhouseEnumerator(fields, predicates, order, offset, fetch, name);
      }
    };
  }

  public static class ClickhouseQueryable<T> extends AbstractTableQueryable<T> {

    protected ClickhouseQueryable(QueryProvider queryProvider, SchemaPlus schema,
        QueryableTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    @Override
    public Enumerator<T> enumerator() {
      final Enumerable<T> enumerable =
          (Enumerable<T>) getTable().clickhouseQuery(ImmutableList.of(), ImmutableList.of(),
              ImmutableList.of(), ImmutableList.of(),
              null, null);
      return enumerable.enumerator();
    }

    public ClickhouseTable getTable() {
      return (ClickhouseTable) table;
    }

    /**
     * Called via code-generation.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> clickhouseQuery(List<Map.Entry<String, Class>> fields,
        List<Map.Entry<String, String>> selectFields,
        List<String> predicates,
        List<String> order,
        Integer offset,
        Integer fetch) {
      return getTable().clickhouseQuery(fields, selectFields, predicates,
          order, offset, fetch);
    }
  }

}
