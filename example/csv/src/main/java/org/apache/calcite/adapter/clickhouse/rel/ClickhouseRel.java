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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface ClickhouseRel extends RelNode {
  // We can add implement() here...

  void implement(Implementor implementor);

  class Implementor {
    RelOptTable relOptTable;
    ClickhouseTable clickhouseTable;
    ClickhouseJoin clickhouseJoin;

    final Map<String, String> selectFields = new LinkedHashMap<>();
    final List<String> whereClause = new ArrayList<>();
    int offset = 0;
    int fetch = -1;
    final List<String> order = new ArrayList<>();

    public void visitChild(RelNode input) {
      ((ClickhouseRel) input).implement(this);
    }

    public void addFields(Map<String, String> fields) {
      selectFields.putAll(fields);
    }

    public void addPredicates(List<String> predicates) {
      whereClause.addAll(predicates);
    }
  }
}
