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

import org.apache.calcite.adapter.clickhouse.rel.ClickhouseConvention;
import org.apache.calcite.adapter.clickhouse.rel.ClickhouseRules;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;

public class Main {

  private static final boolean USE_HEP_PLANNER = false;

  public static void main(String[] args) throws Exception {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
    ClickhouseSchema schema = new ClickhouseSchema();
    rootSchema.add("clickhouse", schema);
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);
    traitDefs.add(schema.getConvention().getTraitDef());

    FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.config().withCaseSensitive(false))
        .defaultSchema(rootSchema.plus())
        .traitDefs(traitDefs)
        .typeSystem(RelDataTypeSystem.DEFAULT)
        .costFactory(RelOptCostImpl.FACTORY)
        .build();

    // Warmup JIT to do some simple benchmarks later
    for (int i = 0; i < 50; i++) {
      parseAndOptimize(frameworkConfig, schema, "SELECT id FROM clickhouse.t1", true);
    }

    parseAndOptimize(frameworkConfig, schema, "SELECT id FROM clickhouse.t1", false);
    parseAndOptimize(frameworkConfig, schema, "SELECT * FROM clickhouse.t1 where t1.val = 'blub'"
        , false);
    parseAndOptimize(frameworkConfig, schema, "SELECT * FROM clickhouse.t1 " +
        "join clickhouse.t2 t2 on t1.id = t2.id where t2.val = 'bla'", false);

    // FIXME: decide if we want to use the Calcite framework for executing the plan...
//    PreparedStatement statement = RelRunners.run(relNode);
//    ResultSet result = statement.executeQuery();
//    System.out.println(result.next());
  }

  private static void parseAndOptimize(FrameworkConfig frameworkConfig,
      ClickhouseSchema schema, String sql, boolean warmup) throws Exception {
    if (!warmup) {
      System.out.println("******");
      System.out.println("SQL: " + sql);
    }
    long before = System.nanoTime();
    // Instantiate a planner, parse the SQL, validate it, create a relational tree out of it.
    Planner planner = Frameworks.getPlanner(frameworkConfig);
    SqlNode sqlNode = planner.parse(sql);
    SqlNode validatedNode = planner.validate(sqlNode);
    RelRoot relRoot = planner.rel(validatedNode);
    RelNode relNode = relRoot.project();
    final RelWriter relWriter = new RelWriterImpl(new PrintWriter(System.out),
        SqlExplainLevel.ALL_ATTRIBUTES, false);
    if (!warmup) {
      System.out.println("Corresponding logical plan");
      relNode.explain(relWriter);
    }
    // Finally it's time to optimize our tree.
    RelNode optimizedTree = optimize(relRoot.rel, schema.getConvention());
    long after = System.nanoTime();
    if (!warmup) {
      System.out.println("Optimized tree");
      optimizedTree.explain(relWriter);
      System.out.printf("The whole magic took %s seconds\n", (after - before) / 1000000000d);
      System.out.println("******");
    }
  }

  private static RelNode optimize(RelNode rootRel, ClickhouseConvention convention) {
    if (USE_HEP_PLANNER) {
      final HepProgram hepProgram = new HepProgramBuilder()
          .addRuleCollection(RelOptRules.BASE_RULES)
          .addRuleCollection(ClickhouseRules.rules(convention))
          .build();
      final HepPlanner planner = new HepPlanner(hepProgram);
      planner.setRoot(rootRel);
      return planner.findBestExp();
    } else {
      RelOptPlanner planner = rootRel.getCluster().getPlanner();
      planner.setRoot(rootRel);
      List<RelOptRule> rules = new ArrayList<>();
      rules.addAll(ClickhouseRules.rules(convention));
      Program prog = Programs.ofRules(rules);
      RelTraitSet traits = planner.emptyTraitSet()
          .replace(convention);
      return prog.run(planner, rootRel, traits, emptyList(), emptyList());
    }

  }
}
