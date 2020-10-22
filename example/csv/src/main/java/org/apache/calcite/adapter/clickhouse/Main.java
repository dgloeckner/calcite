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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

import java.io.PrintWriter;

public class Main {

  private static final boolean USE_HEP_PLANNER = true;

  public static void main(String[] args) throws Exception {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
    ClickhouseSchema schema = new ClickhouseSchema();
    rootSchema.add("clickhouse", schema);
    RuleSet ruleSet = RuleSets.ofList(ClickhouseRules.rules(schema.getConvention()));

    FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.config().withCaseSensitive(false))
        .defaultSchema(rootSchema.plus())
        .ruleSets(ruleSet)
        .build();

    // Warmup JIT to do some simple benchmarks later
    for (int i = 0; i < 50; i++) {
      parseAndOptimize(frameworkConfig, schema, "SELECT id FROM clickhouse.t1", true);
    }

    parseAndOptimize(frameworkConfig, schema, "SELECT id FROM clickhouse.t1", false);
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
    RelTraitSet traits = RelTraitSet.createEmpty();
    traits.plus(schema.getConvention());
    // Finally it's time to optimize our tree.
    RelNode optimizedTree;
    if (USE_HEP_PLANNER) {
      optimizedTree = optimizeWithHepPlanner(relRoot.rel, schema.getConvention());
    } else {
      optimizedTree = planner.transform(0, traits, relRoot.rel);
    }
    long after = System.nanoTime();
    if (!warmup) {
      System.out.println("Optimized tree");
      optimizedTree.explain(relWriter);
      System.out.printf("The whole magic took %s seconds\n", (after - before) / 1000000000d);
      System.out.println("******");
    }
  }

  private static RelNode optimizeWithHepPlanner(RelNode rootRel, ClickhouseConvention convention) {
    final HepProgram hepProgram = new HepProgramBuilder()
        .addRuleCollection(ClickhouseRules.rules(convention))
        .addRuleInstance(CoreRules.CALC_SPLIT)
        .addRuleInstance(CoreRules.FILTER_SCAN)
        .addRuleInstance(CoreRules.FILTER_INTERPRETER_SCAN)
        .addRuleInstance(CoreRules.PROJECT_TABLE_SCAN)
        .addRuleInstance(CoreRules.PROJECT_INTERPRETER_TABLE_SCAN)
        .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .build();
    final HepPlanner planner = new HepPlanner(hepProgram);
    planner.setRoot(rootRel);
    rootRel = planner.findBestExp();
    return rootRel;
  }
}
