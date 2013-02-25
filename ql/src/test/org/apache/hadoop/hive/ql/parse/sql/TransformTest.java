/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse.sql;

/**
 * Test transformer.
 * TransformTest.
 *
 */
public class TransformTest extends BaseSQLTest {

  public void testPreapreQInfo1() {
    String sql = "select key from src group by key;";
    String qTree = "(root[src])";
    testQInfo(sql, qTree);
  }

  public void testPrepareQInfo2() {
    String sql = "select aa, sum(bb) from (select aa,bb,cc from (select aa,bb,cc from t3 where dd!=0 ) subq2 ) subq1 where subq1.cc!=0 and subq1.cc > all (select x from (select x, count(1) as y from t4 group by x) subq3 where subq3.y = 2);";
    String qTree = "(root([subq1]([subq2][t3])[t4]))";
    testQInfo(sql, qTree);
  }

  public void testPrepareQInfoUnion() {
    String sql = "select c from src_sql_t1 union all select z from src_sql_t3;";
    String qTree = "(root[src_sql_t1][src_sql_t3])";
    testQInfo(sql, qTree);
  }

  public void testPrepareFilterBlock() {
    String sql = "select a, sum(b) from t1 where c!=0 and c!=1 and c > all (select c from t2 where x = 2 or t2.y > t1.d) and c in (select c from t3 where y = 3) or exists (select d from t4 where t4.d = t1.d or t4.e>2 ) group by a having sum(b) >  (select max(bb) from t5 where z != 4);";
    String qTree = "[root [QueryBlock_select [WhereFilterBlock_where [OrFilterBlock_or [AndFilterBlock_and [AndFilterBlock_and UnCorrelatedFilterBlock_and [SubQFilterBlock_> [QueryBlock_select [WhereFilterBlock_where [OrFilterBlock_or UnCorrelatedFilterBlock_= CorrelatedFilterBlock_> ] ] ] ] ] [SubQFilterBlock_in [QueryBlock_select [WhereFilterBlock_where UnCorrelatedFilterBlock_= ] ] ] ] [SubQFilterBlock_exists [QueryBlock_select [WhereFilterBlock_where [OrFilterBlock_or CorrelatedFilterBlock_= UnCorrelatedFilterBlock_> ] ] ] ] ] ] [HavingFilterBlock_having [SubQFilterBlock_> [QueryBlock_select [WhereFilterBlock_where UnCorrelatedFilterBlock_!= ] ] ] ] ] ]";
    super.testPrepareFilterBlock(sql, qTree);
  }

  public void testGreaterThanAll() {
    String sql = "select a from t1 where b > all (select b from t2 where t2.c > 100);";
    String tTree = "(STATEMENTS (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME t1)))) (join cross (TABLE_REF_ELEMENT (ALIAS panthera_0) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME t2)))))) (SELECT_LIST (SELECT_ITEM (EXPR (STANDARD_FUNCTION (max (ARGUMENTS (ARGUMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT b)))))))) (ALIAS panthera_1))) (where (LOGIC_EXPR (> (CASCATED_ELEMENT (ANY_ELEMENT t2 c)) 100)))))))))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT a))))) (where (LOGIC_EXPR (> (CASCATED_ELEMENT (ANY_ELEMENT b)) (CASCATED_ELEMENT (ANY_ELEMENT panthera_0 panthera_1)))))))))";
    testFilterBlockTransformer(sql, tTree);
  }
}
