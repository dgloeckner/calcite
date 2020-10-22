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
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

public class ClickhouseSchema  extends AbstractSchema {

  private final ClickhouseConvention convention = new ClickhouseConvention();

  @Override
  protected Map<String, Table> getTableMap() {
    Map<String, Table> tables = new HashMap<>();
    tables.put("T1", new ClickhouseTable(this));
    tables.put("T2", new ClickhouseTable(this));
    return tables;
  }

  public ClickhouseConvention getConvention() {
    return convention;
  }
}
