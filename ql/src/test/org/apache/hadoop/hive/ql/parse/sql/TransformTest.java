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

  public void testPreapreQInfo1() throws SqlXlateException {
    String sql = "select key from src group by key;";
    String qTree = "(root[src])";
    testQInfo(sql, qTree);
  }

  public void testPrepareQInfo2() throws SqlXlateException {
    String sql = "select aa, sum(bb) from (select aa,bb,cc from (select aa,bb,cc from t3 where dd!=0 ) subq2 ) subq1 where subq1.cc!=0 and subq1.cc > all (select x from (select x, count(1) as y from t4 group by x) subq3 where subq3.y = 2);";
    String qTree = "(root([subq1]([subq2][t3])[t4]))";
    testQInfo(sql, qTree);
  }

  public void testPrepareQInfoUnion() throws SqlXlateException {
    String sql = "select c from src_sql_t1 union all select z from src_sql_t3;";
    String qTree = "(root[src_sql_t1][src_sql_t3])";
    testQInfo(sql, qTree);
  }

  public void testPrepareFilterBlock() throws SqlXlateException {
    String sql = "select a, sum(b) from t1 where c!=0 and c!=1 and c > all (select c from t2 where x = 2 or t2.y > t1.d) and c in (select c from t3 where y = 3) or exists (select d from t4 where t4.d = t1.d or t4.e>2 ) group by a having sum(b) >  (select max(bb) from t5 where z != 4);";
    String qTree = "[root [QueryBlock_select [WhereFilterBlock_where [OrFilterBlock_or [AndFilterBlock_and [AndFilterBlock_and UnCorrelatedFilterBlock_and [SubQFilterBlock_> [QueryBlock_select [WhereFilterBlock_where [OrFilterBlock_or UnCorrelatedFilterBlock_= CorrelatedFilterBlock_> ] ] ] ] ] [SubQFilterBlock_in [QueryBlock_select [WhereFilterBlock_where UnCorrelatedFilterBlock_= ] ] ] ] [SubQFilterBlock_exists [QueryBlock_select [WhereFilterBlock_where [OrFilterBlock_or CorrelatedFilterBlock_= UnCorrelatedFilterBlock_> ] ] ] ] ] ] [HavingFilterBlock_having [SubQFilterBlock_> [QueryBlock_select [WhereFilterBlock_where UnCorrelatedFilterBlock_!= ] ] ] ] ] ]";
    super.testPrepareFilterBlock(sql, qTree);
  }


  public void testGreaterThanAll() throws SqlXlateException {
    String sql = "select a from t1 where b > all (select b from t2 where t2.c > 100);";
    String tTree = "(STATEMENTS (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME t1)))) (join cross (TABLE_REF_ELEMENT (ALIAS panthera_0) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME t2)))))) (SELECT_LIST (SELECT_ITEM (EXPR (STANDARD_FUNCTION (max (ARGUMENTS (ARGUMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT b)))))))) (ALIAS panthera_1))) (where (LOGIC_EXPR (> (CASCATED_ELEMENT (ANY_ELEMENT t2 c)) 100)))))))))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT a))))) (where (LOGIC_EXPR (> (CASCATED_ELEMENT (ANY_ELEMENT b)) (CASCATED_ELEMENT (ANY_ELEMENT panthera_0 panthera_1)))))))))";
    testFilterBlockTransformer(sql, tTree);
  }


  public void testEqualsC() throws SqlXlateException {
    String sql = "select count(ps_partkey) from partsupp join part on p_partkey = ps_partkey where ps_supplycost = (select min(ps_supplycost)  from partsupp where part.p_partkey =partsupp.ps_partkey);";
    String tTree = "(STATEMENTS (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME partsupp)))) (join (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME part)))) (on (LOGIC_EXPR (= (CASCATED_ELEMENT (ANY_ELEMENT p_partkey)) (CASCATED_ELEMENT (ANY_ELEMENT ps_partkey)))))) (join (TABLE_REF_ELEMENT (ALIAS panthera_0) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME partsupp)))))) (SELECT_LIST (SELECT_ITEM (EXPR (STANDARD_FUNCTION (min (ARGUMENTS (ARGUMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT ps_supplycost)))))))) (ALIAS panthera_1)) (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT partsupp ps_partkey))) (ALIAS panthera_2))) (group (GROUP_BY_ELEMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT partsupp ps_partkey))))))))))) (on (LOGIC_EXPR (= (CASCATED_ELEMENT (ANY_ELEMENT part p_partkey)) (CASCATED_ELEMENT (ANY_ELEMENT panthera_2)))))))) (SELECT_LIST (SELECT_ITEM (EXPR (STANDARD_FUNCTION (count (EXPR (CASCATED_ELEMENT (ANY_ELEMENT ps_partkey)))))))) (where (LOGIC_EXPR (= (CASCATED_ELEMENT (ANY_ELEMENT ps_supplycost)) (CASCATED_ELEMENT (ANY_ELEMENT panthera_1)))))))))";
    testFilterBlockTransformer(sql, tTree);
  }

  public void testGreaterThanHaving() throws SqlXlateException {
    String sql = "select ps_partkey,sum(ps_supplycost ) as value from partsupp group by ps_partkey having sum(ps_supplycost) > (select  sum(ps_supplycost ) * 0.000001000000  from  partsupp);";
    String tTree = "(STATEMENTS (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (ALIAS panthera_2) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME partsupp)))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT ps_partkey))) (ALIAS panthera_0)) (SELECT_ITEM (EXPR (STANDARD_FUNCTION (sum (ARGUMENTS (ARGUMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT ps_supplycost)))))))) (ALIAS value)) (SELECT_ITEM (EXPR (STANDARD_FUNCTION (sum (ARGUMENTS (ARGUMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT ps_supplycost)))))))) (ALIAS panthera_1))) (group (GROUP_BY_ELEMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT ps_partkey))))))))))) (join cross (TABLE_REF_ELEMENT (ALIAS panthera_3) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME partsupp)))))) (SELECT_LIST (SELECT_ITEM (EXPR (* (STANDARD_FUNCTION (sum (ARGUMENTS (ARGUMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT ps_supplycost))))))) 0.000001000000)) (ALIAS panthera_4))))))))) (on (LOGIC_EXPR (> (CASCATED_ELEMENT (ANY_ELEMENT panthera_1)) (CASCATED_ELEMENT (ANY_ELEMENT panthera_4)))))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT panthera_0))) (ALIAS panthera_5)) (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT value))) (ALIAS panthera_6)))))))";
    testFilterBlockTransformer(sql, tTree);
  }

  public void testExist() throws SqlXlateException {
    String sql = "select count(o_orderkey)  from orders where  exists (select  * from lineitem where  lineitem.l_orderkey = orders.o_orderkey );";
    String tTree = "(STATEMENTS (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (ALIAS panthera_1) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME orders)))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT o_orderkey))) (ALIAS panthera_0))))))))) (join leftsemi (TABLE_REF_ELEMENT (ALIAS panthera_2) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME lineitem)))))) *)))))) (on (LOGIC_EXPR (= (CASCATED_ELEMENT (ANY_ELEMENT lineitem l_orderkey)) (CASCATED_ELEMENT (ANY_ELEMENT orders o_orderkey)))))))) (SELECT_LIST (SELECT_ITEM (EXPR (STANDARD_FUNCTION (count (EXPR (CASCATED_ELEMENT (ANY_ELEMENT o_orderkey)))))) (ALIAS panthera_3)))))))";
    testFilterBlockTransformer(sql, tTree);
  }

  public void testEqualsUC() throws SqlXlateException {
    String sql = "select supplier_no from  revenue0 where total_revenue = (select  max(total_revenue)  from  revenue0 );";
    String tTree = "(STATEMENTS (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (ALIAS panthera_2) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME revenue0)))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT supplier_no))) (ALIAS panthera_0)) (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT total_revenue))) (ALIAS panthera_1))))))))) (join (TABLE_REF_ELEMENT (ALIAS panthera_3) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME revenue0)))))) (SELECT_LIST (SELECT_ITEM (EXPR (STANDARD_FUNCTION (max (ARGUMENTS (ARGUMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT total_revenue)))))))) (ALIAS panthera_4))))))))) (on (LOGIC_EXPR (= (CASCATED_ELEMENT (ANY_ELEMENT panthera_1)) (CASCATED_ELEMENT (ANY_ELEMENT panthera_4)))))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT panthera_0))) (ALIAS panthera_5)))))))";
    testFilterBlockTransformer(sql, tTree);
  }

  public void testNotInUC() throws SqlXlateException {
    String sql = "select count(ps_suppkey) from partsupp where ps_suppkey not in (select s_suppkey from supplier where s_comment like '%Customer%Complaints%');";
    String tTree = "(STATEMENTS (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (ALIAS panthera_2) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME partsupp)))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT ps_suppkey))) (ALIAS panthera_0)) (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT ps_suppkey))) (ALIAS panthera_1))))))))) (join cross (TABLE_REF_ELEMENT (ALIAS panthera_3) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME supplier)))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ROUTINE_CALL (ROUTINE_NAME collect_set) (ARGUMENTS (ARGUMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT s_suppkey)))))))) (ALIAS panthera_4))) (where (LOGIC_EXPR (like (CASCATED_ELEMENT (ANY_ELEMENT s_comment)) (EXPR '%Customer%Complaints%'))))))))))))) (SELECT_LIST (SELECT_ITEM (EXPR (STANDARD_FUNCTION (count (EXPR (CASCATED_ELEMENT (ANY_ELEMENT ps_suppkey)))))) (ALIAS panthera_5))) (where (LOGIC_EXPR (not (CASCATED_ELEMENT (ROUTINE_CALL (ROUTINE_NAME array_contains) (ARGUMENTS (ARGUMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT panthera_1)))) (ARGUMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT panthera_4))))))))))))))";
    testFilterBlockTransformer(sql, tTree);
  }

  public void testLessThanC() throws SqlXlateException {
    String sql = "select  sum(l_extendedprice) / 7.0 as avg_yearly from part where l_quantity < (select  0.2 * avg(l_quantity)  from    lineitem  where   lineitem.l_partkey = part.p_partkey);";
    String tTree = "(STATEMENTS (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME part)))) (join (TABLE_REF_ELEMENT (ALIAS panthera_0) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME lineitem)))))) (SELECT_LIST (SELECT_ITEM (EXPR (* 0.2 (STANDARD_FUNCTION (avg (ARGUMENTS (ARGUMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT l_quantity))))))))) (ALIAS panthera_1)) (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT lineitem l_partkey))) (ALIAS panthera_2))) (group (GROUP_BY_ELEMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT lineitem l_partkey))))))))))) (on (LOGIC_EXPR (= (CASCATED_ELEMENT (ANY_ELEMENT part p_partkey)) (CASCATED_ELEMENT (ANY_ELEMENT panthera_2)))))))) (SELECT_LIST (SELECT_ITEM (EXPR (/ (STANDARD_FUNCTION (sum (ARGUMENTS (ARGUMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT l_extendedprice))))))) 7.0)) (ALIAS avg_yearly))) (where (LOGIC_EXPR (< (CASCATED_ELEMENT (ANY_ELEMENT l_quantity)) (CASCATED_ELEMENT (ANY_ELEMENT panthera_1)))))))))";
    testFilterBlockTransformer(sql, tTree);
  }

  public void testINUC() throws SqlXlateException {
    String sql = "select o_orderkey from orders where o_orderkey in (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity)>300);";
    String tTree = "(STATEMENTS (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (ALIAS panthera_2) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME orders)))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT o_orderkey))) (ALIAS panthera_0)) (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT o_orderkey))) (ALIAS panthera_1))))))))) (join leftsemi (TABLE_REF_ELEMENT (ALIAS panthera_3) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME lineitem)))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT l_orderkey))) (ALIAS panthera_4))) (group (GROUP_BY_ELEMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT l_orderkey)))) (having (LOGIC_EXPR (> (STANDARD_FUNCTION (sum (ARGUMENTS (ARGUMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT l_quantity))))))) 300)))))))))) (on (LOGIC_EXPR (= (CASCATED_ELEMENT (ANY_ELEMENT panthera_1)) (CASCATED_ELEMENT (ANY_ELEMENT panthera_4)))))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT panthera_0))) (ALIAS panthera_5)))))))";
    testFilterBlockTransformer(sql, tTree);
  }

  public void testNotExistsC() throws SqlXlateException {
    String sql = "select  count(l1.l_orderkey)  from lineitem l1 where not exists (select * from lineitem l2 where l1.l_orderkey = l2.l_orderkey);";
    String tTree = "(STATEMENTS (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (ALIAS panthera_2) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME orders)))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT o_orderkey))) (ALIAS panthera_0)) (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT o_orderkey))) (ALIAS panthera_1))))))))) (join leftsemi (TABLE_REF_ELEMENT (ALIAS panthera_3) (TABLE_EXPRESSION (SELECT_MODE (SELECT_STATEMENT (SUBQUERY (select (from (TABLE_REF (TABLE_REF_ELEMENT (TABLE_EXPRESSION (DIRECT_MODE (TABLEVIEW_NAME lineitem)))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT l_orderkey))) (ALIAS panthera_4))) (group (GROUP_BY_ELEMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT l_orderkey)))) (having (LOGIC_EXPR (> (STANDARD_FUNCTION (sum (ARGUMENTS (ARGUMENT (EXPR (CASCATED_ELEMENT (ANY_ELEMENT l_quantity))))))) 300)))))))))) (on (LOGIC_EXPR (= (CASCATED_ELEMENT (ANY_ELEMENT panthera_1)) (CASCATED_ELEMENT (ANY_ELEMENT panthera_4)))))))) (SELECT_LIST (SELECT_ITEM (EXPR (CASCATED_ELEMENT (ANY_ELEMENT panthera_0))) (ALIAS panthera_5)))))))";
    testFilterBlockTransformer(sql, tTree);
  }
}
