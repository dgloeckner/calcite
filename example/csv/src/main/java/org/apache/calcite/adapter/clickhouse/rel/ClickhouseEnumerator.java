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

import org.apache.calcite.linq4j.Enumerator;

import java.util.List;
import java.util.Map;


public class ClickhouseEnumerator implements Enumerator<Object> {

  private final List<Map.Entry<String, Class>> fields;

  private final String tableName;

  int rows = 1;

  // Pass ResultSet results, RelProtoDataType protoRowType
  public ClickhouseEnumerator(List<Map.Entry<String, Class>> fields, List<String> predicates,
      List<String> order, Integer offset, Integer fetch, String tableName) {
    this.fields = fields;
    this.tableName = tableName;
  }

  @Override
  public Object current() {
    Object[] row = new Object[fields.size()];
    for (int i = 0; i < row.length; i++) {
      Map.Entry<String, Class> field = fields.get(i);
      if(field.getValue() == int.class) {
        row[i] = tableName.toLowerCase().equals("t1") ? 1 : 2;
      } else {
        row[i] = "Bla";
      }
    }
    return row;
  }

  @Override
  public boolean moveNext() {
    return --rows >= 0;
  }

  @Override
  public void reset() {
    rows = 1;
  }

  @Override
  public void close() {

  }
}
