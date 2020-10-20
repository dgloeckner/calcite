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
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.ClickHouseSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelRunner;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class OptimizerTest {


  @Test
  void testOptimize() throws SqlParseException {
    SqlParser parser = SqlParser.create("SELECT * FROM emps e join emps2 e2 where e1.name = e2.name");
    SqlNode node = parser.parseQuery();
    System.out.println(node);
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
    CalcitePrepareImpl prep = new CalcitePrepareImpl();

    rootSchema.add("EMPS", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        JavaTypeFactory jtf = (JavaTypeFactory) typeFactory;
        return jtf.createStructType(Arrays.asList(jtf.createSqlType(SqlTypeName.CHAR)),
            Arrays.asList("NAME"));
      }
    });
    rootSchema.add("EMPS2", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        JavaTypeFactory jtf = (JavaTypeFactory) typeFactory;
        return jtf.createStructType(Arrays.asList(jtf.createSqlType(SqlTypeName.CHAR), jtf.createSqlType(SqlTypeName.CHAR)),
            Arrays.asList("NAME2", "VAL"));
      }
    });
    rootSchema.add("REPLACEMENT", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        JavaTypeFactory jtf = (JavaTypeFactory) typeFactory;
        return jtf.createStructType(Arrays.asList(jtf.createSqlType(SqlTypeName.CHAR)),
            Arrays.asList("NAME3"));
      }
    });
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(new Properties());
    CalcitePrepare.Context context = new CalcitePrepare.Context() {
      @Override
      public JavaTypeFactory getTypeFactory() {
        return new JavaTypeFactoryImpl();
      }

      @Override
      public CalciteSchema getRootSchema() {
        return rootSchema;
      }

      @Override
      public CalciteSchema getMutableRootSchema() {
        return rootSchema;
      }

      @Override
      public List<String> getDefaultSchemaPath() {
        return Arrays.asList("default");
      }

      @Override
      public CalciteConnectionConfig config() {
        return config;
      }

      @Override
      public CalcitePrepare.SparkHandler spark() {
        return CalcitePrepare.Dummy.getSparkHandler(false);
      }

      @Override
      public DataContext getDataContext() {
        throw new UnsupportedOperationException();
      }

      @Override
      public List<String> getObjectPath() {
        return null;
      }

      @Override
      public RelRunner getRelRunner() {
        throw new UnsupportedOperationException();
      }
    };
    CalcitePrepare.ConvertResult r = prep.convert(context, "SELECT * FROM emps e1 join emps2 e2 on e1.name = e2.name2 where e2.val = 'bla'");

    System.out.println("got " + r.root);
    Opti opti = new Opti();
    RelRoot optimized = opti.optimize(r.root, null);
    System.out.println("Final " + optimized);
    System.out.println("Clickhouse unoptimized: " +
        SqlParser.create("SELECT * FROM emps e1 join emps2 e2 on e1.name = e2.name2 where e2.val = 'bla'").parseQuery().toSqlString(ClickHouseSqlDialect.DEFAULT));
    System.out.println("Clickhouse optimized: " + toSql(optimized.rel, ClickHouseSqlDialect.DEFAULT));
  }

  /** Converts a relational expression to SQL in a given dialect. */
  private static String toSql(RelNode root, SqlDialect dialect) {
    final RelToSqlConverter converter = new RelToSqlConverter(dialect);
    final SqlNode sqlNode = converter.visitRoot(root).asStatement();
    return sqlNode.toSqlString(dialect).getSql();
  }
}
