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

import org.apache.calcite.adapter.clickhouse.ClickhouseTable;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.AbstractEnumerable2;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Consumer;

public class ClickhouseToEnumerableConverter extends ConverterImpl
    implements EnumerableRel {

  protected ClickhouseToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits,
      RelNode child) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, child);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ClickhouseToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.1);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Generates a call to "clickhouseQuery" with the appropriate fields and predicates
    ClickhouseRel.Implementor clickhouseImplementor = new ClickhouseRel.Implementor();
    clickhouseImplementor.visitChild(getInput());
    final BlockBuilder list = new BlockBuilder();
    final RelDataType rowType = getRowType();
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(), rowType,
            pref.prefer(JavaRowFormat.ARRAY));
    final Expression fields =
        list.append("fields",
            constantArrayList(
                Pair.zip(clickhouseFieldNames(rowType),
                    new AbstractList<Class>() {
                      @Override public Class get(int index) {
                        return physType.fieldClass(index);
                      }

                      @Override public int size() {
                        return rowType.getFieldCount();
                      }
                    }),
                Pair.class));

    List<Map.Entry<String, String>> selectList = new ArrayList<>();
    for (Map.Entry<String, String> entry
        : Pair.zip(clickhouseImplementor.selectFields.keySet(),
        clickhouseImplementor.selectFields.values())) {
      selectList.add(entry);
    }

    final Expression selectFields =
        list.append("selectFields", constantArrayList(selectList, Pair.class));
    final Expression table =
        list.append("table",
            clickhouseImplementor.relOptTable.getExpression(
                ClickhouseTable.ClickhouseQueryable.class));
    final Expression predicates =
        list.append("predicates",
            constantArrayList(clickhouseImplementor.whereClause, String.class));
    final Expression order =
        list.append("order",
            constantArrayList(clickhouseImplementor.order, String.class));
    final Expression offset =
        list.append("offset",
            Expressions.constant(clickhouseImplementor.offset));
    final Expression fetch =
        list.append("fetch",
            Expressions.constant(clickhouseImplementor.fetch));

    Method queryMethod = Types.lookupMethod(ClickhouseTable.ClickhouseQueryable.class, "clickhouseQuery",
        List.class, List.class, List.class, List.class, Integer.class, Integer.class);
    Expression enumerable =
        list.append("enumerable",
            Expressions.call(table,
                queryMethod, fields,
                selectFields, predicates, order, offset, fetch));
    if (CalciteSystemProperty.DEBUG.value()) {
      System.out.println("Cassandra: " + predicates);
    }
    Hook.QUERY_PLAN.run(predicates);
    list.add(
        Expressions.return_(null, enumerable));
    return implementor.result(physType, list.toBlock());
  }

  /** E.g. {@code constantArrayList("x", "y")} returns
   * "Arrays.asList('x', 'y')". */
  private static <T> MethodCallExpression constantArrayList(List<T> values,
      Class clazz) {
    return Expressions.call(
        BuiltInMethod.ARRAYS_AS_LIST.method,
        Expressions.newArrayInit(clazz, constantList(values)));
  }

  /** E.g. {@code constantList("x", "y")} returns
   * {@code {ConstantExpression("x"), ConstantExpression("y")}}. */
  private static <T> List<Expression> constantList(List<T> values) {
    return Util.transform(values, Expressions::constant);
  }

  static List<String> clickhouseFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(rowType.getFieldNames(),
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }
}
