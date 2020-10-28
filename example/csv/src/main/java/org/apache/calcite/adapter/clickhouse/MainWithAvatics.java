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

import java.sql.*;
import java.util.Properties;

public class MainWithAvatics {

  public static void main(String[] args) throws Exception {
    Properties info = new Properties();
    info.put("model",
        "inline:"
            + "{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       type: 'custom',\n"
            + "       name: 'CLICKHOUSE',\n"
            + "       factory: 'org.apache.calcite.adapter.clickhouse.ClickhouseSchemaFactory',\n"
            + "       operand: {\n"
            + "         directory: '/does/not/exist'\n"
            + "       }\n"
            + "     }\n"
            + "   ]\n"
            + "}");
    info.put("conformance", "BABEL");

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info)) {
      execSql(connection, "explain plan for SELECT id, int1 + int2 as schnitzel FROM " +
          "clickhouse.t1 where t1.string2 = 'bla' and t1.int1 = 42 order by t1.int1 limit 10, 20");
      //execSql(connection, "SELECT * FROM " +
      //    "clickhouse.t1 t1 join clickhouse.t2 t2 on t1.id = t2.id");
    }
  }

  private static void execSql(Connection connection, String query) throws SQLException {
    try (Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery(query)) {
      System.out.println("*** " + query + " ****");
      if (rs.next()) {
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
          System.out.print(rs.getObject(i));
          System.out.print("|");
        }
        System.out.println();
      }
    }
  }
}
