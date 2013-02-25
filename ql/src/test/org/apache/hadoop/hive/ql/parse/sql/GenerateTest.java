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
 * Test generator.
 * GenerateTest.
 *
 */

public class GenerateTest extends BaseSQLTest {

  public void testWhere() {
    String sql = "select a from abcd where d>4;";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME abcd))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL a))) (TOK_WHERE (> (TOK_TABLE_OR_COL d) 4))))";
    testGenerateText(sql, ast);
  }

  public void testGroupBy() {
    String sql = "select key from src group by key;";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL key))) (TOK_GROUPBY (TOK_TABLE_OR_COL key))))";
    testGenerateText(sql, ast);
  }

  public void testCount() {
    String sql = "select count(key) from src group by key;";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_FUNCTION count (TOK_TABLE_OR_COL key)))) (TOK_GROUPBY (TOK_TABLE_OR_COL key))))";
    testGenerateText(sql, ast);
  }


  public void testStar() {
    String sql = "select * from table1";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME table1))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR TOK_ALLCOLREF))))";
    testGenerateText(sql, ast);
  }

  public void testOrder() {
    String sql = "select * from table1 order by a, b;";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME table1))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR TOK_ALLCOLREF)) (TOK_ORDERBY (TOK_TABSORTCOLNAMEASC (TOK_TABLE_OR_COL a)) (TOK_TABSORTCOLNAMEASC (TOK_TABLE_OR_COL b)))))";
    testGenerateText(sql, ast);
  }

  public void testAlias() {
    String sql = "select src.* from table1 src;";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME table1) src)) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_ALLCOLREF (TOK_TABNAME src))))))";
    String type = " [5 [9 [15 [232 262] 262]] [4 [18 [13 158]] [6 [8 [19 [232 262]]]]]]";
    testGenerateText(sql, ast);
  }

  public void testUnion() {
    String sql = "select unionsrc.key, unionsrc.value from (select s1.key as key, s1.value as value from src s1 union  all select s2.key as key, s2.value as value from src s2 union  all select s3.key as key, s3.value as value from src s3) unionsrc;";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_SUBQUERY (TOK_UNION (TOK_UNION (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src) s1)) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL s1) key) key) (TOK_SELEXPR (. (TOK_TABLE_OR_COL s1) value) value)))) (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src) s2)) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL s2) key) key) (TOK_SELEXPR (. (TOK_TABLE_OR_COL s2) value) value))))) (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src) s3)) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL s3) key) key) (TOK_SELEXPR (. (TOK_TABLE_OR_COL s3) value) value))))) unionsrc)) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL unionsrc) key)) (TOK_SELEXPR (. (TOK_TABLE_OR_COL unionsrc) value)))))";
    String type = " [5 [9 [16 [58 [58 [5 [9 [15 [232 262] 262]] [4 [18 [13 158]] [6 [8 [311 [20 262] 262] 262] [8 [311 [20 262] 262] 262]]]] [5 [9 [15 [232 262] 262]] [4 [18 [13 158]] [6 [8 [311 [20 262] 262] 262] [8 [311 [20 262] 262] 262]]]]] [5 [9 [15 [232 262] 262]] [4 [18 [13 158]] [6 [8 [311 [20 262] 262] 262] [8 [311 [20 262] 262] 262]]]]] 262]] [4 [18 [13 158]] [6 [8 [311 [20 262] 262]] [8 [311 [20 262] 262]]]]]";
    super.testGenerateType(sql, type);
    testGenerateText(sql, ast);
  }

  public void testHaving() {
    String sql = "select key from src group by key having max(value) > \"val_255\";";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL key))) (TOK_GROUPBY (TOK_TABLE_OR_COL key)) (TOK_HAVING (> (TOK_FUNCTION max (TOK_TABLE_OR_COL value)) \"val_255\"))))";
    String type = " [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [20 262]]] [52 [20 262]] [53 [380 [21 262 [20 262]] 243]]]]";
    super.testGenerateType(sql, type);
    testGenerateText(sql, ast);
  }

  public void testAndOr() {
    String sql = "select key, value, ds from pcr_t1 where (ds='2000-04-08' and key=1) or (ds='2000-04-09' and key=2) order by key, value, ds;";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME pcr_t1))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL key)) (TOK_SELEXPR (TOK_TABLE_OR_COL value)) (TOK_SELEXPR (TOK_TABLE_OR_COL ds))) (TOK_WHERE (or (and (= (TOK_TABLE_OR_COL ds) '2000-04-08') (= (TOK_TABLE_OR_COL key) 1)) (and (= (TOK_TABLE_OR_COL ds) '2000-04-09') (= (TOK_TABLE_OR_COL key) 2)))) (TOK_ORDERBY (TOK_TABSORTCOLNAMEASC (TOK_TABLE_OR_COL key)) (TOK_TABSORTCOLNAMEASC (TOK_TABLE_OR_COL value)) (TOK_TABSORTCOLNAMEASC (TOK_TABLE_OR_COL ds)))))";
    testGenerateText(sql, ast);
  }


  public void testGroupByMany() {
    String sql = "select INPUT__FILE__NAME, key, collect_set(BLOCK__OFFSET__INSIDE__FILE) from src group by INPUT__FILE__NAME, key order by key;";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL INPUT__FILE__NAME)) (TOK_SELEXPR (TOK_TABLE_OR_COL key)) (TOK_SELEXPR (TOK_FUNCTION collect_set (TOK_TABLE_OR_COL BLOCK__OFFSET__INSIDE__FILE)))) (TOK_GROUPBY (TOK_TABLE_OR_COL INPUT__FILE__NAME) (TOK_TABLE_OR_COL key)) (TOK_ORDERBY (TOK_TABSORTCOLNAMEASC (TOK_TABLE_OR_COL key)))))";
    testGenerateText(sql, ast);
  }

  public void testJoin() {
    String sql = "select src1.c1, src2.c4 from (select src.key as c1, src.value as c2 from src) src1 join (select src.key as c3, src.value as c4 from src) src2 on src1.c1 = src2.c3 and src1.c1 < 100;";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_JOIN (TOK_SUBQUERY (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) key) c1) (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) value) c2)))) src1) (TOK_SUBQUERY (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) key) c3) (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) value) c4)))) src2) (and (= (. (TOK_TABLE_OR_COL src1) c1) (. (TOK_TABLE_OR_COL src2) c3)) (< (. (TOK_TABLE_OR_COL src1) c1) 100)))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src1) c1)) (TOK_SELEXPR (. (TOK_TABLE_OR_COL src2) c4)))))";
    String type = " [5 [9 [59 [16 [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [311 [20 262] 262] 262] [8 [311 [20 262] 262] 262]]]] 262] [16 [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [311 [20 262] 262] 262] [8 [311 [20 262] 262] 262]]]] 262] [447 [354 [311 [20 262] 262] [311 [20 262] 262]] [379 [311 [20 262] 262] 347]]]] [4 [18 [13 158]] [6 [8 [311 [20 262] 262]] [8 [311 [20 262] 262]]]]]";
    testGenerateText(sql, ast);
    super.testGenerateType(sql, type);
  }

  public void testCase() {
    String sql = "select * from srcpart a join srcpart b on a.key = b.key where a.ds = '2008-04-08' and b.ds = '2008-04-08' and case a.key when '27' then true when '38' then false else null end order by a.key, a.value, a.ds, a.hr, b.key, b.value, b.ds, b.hr;";
    String ast = " [5 [9 [59 [15 [232 262] 262] [15 [232 262] 262] [354 [311 [20 262] 262] [311 [20 262] 262]]]] [4 [18 [13 158]] [6 [8 19]] [24 [447 [447 [354 [311 [20 262] 262] 243] [354 [311 [20 262] 262] 243]] [21 419 [311 [20 262] 262] 243 448 243 449 68]]] [54 [159 [311 [20 262] 262]] [159 [311 [20 262] 262]] [159 [311 [20 262] 262]] [159 [311 [20 262] 262]] [159 [311 [20 262] 262]] [159 [311 [20 262] 262]] [159 [311 [20 262] 262]] [159 [311 [20 262] 262]]]]]";
    testGenerateType(sql, ast);
  }

  public void testLike() {
    String sql = "select a.* from srcpart a where a.ds = '2008-04-08' and not(key > 50 or key < 10) and a.hr like '%2';";
    String ast = " [5 [9 [15 [232 262] 262]] [4 [18 [13 158]] [6 [8 [19 [232 262]]]] [24 [447 [447 [354 [311 [20 262] 262] 243] [256 [257 [380 [20 262] 347] [379 [20 262] 347]]]] [272 [311 [20 262] 262] 243]]]]]";
    testGenerateType(sql, ast);
  }


  public void testMultiJoin() {
    String sql = "select sum(hash(src1.key)), sum(hash(src1.val)), sum(hash(src2.key)) from t1 src1 join t2 src2 on src1.key+1 = src2.key join t2 src3 on src2.key = src3.key;";
    String ast = " [5 [9 [59 [59 [15 [232 262] 262] [15 [232 262] 262] [354 [392 [311 [20 262] 262] 347] [311 [20 262] 262]]] [15 [232 262] 262] [354 [311 [20 262] 262] [311 [20 262] 262]]]] [4 [18 [13 158]] [6 [8 [21 262 [21 262 [311 [20 262] 262]]]] [8 [21 262 [21 262 [311 [20 262] 262]]]] [8 [21 262 [21 262 [311 [20 262] 262]]]]]]]";
    testGenerateType(sql, ast);
  }

  public void testCountAterisk() {
    String sql = "select count(*) from src;";
    String ast = " [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [23 262]]]]]";
    testGenerateType(sql, ast);
  }

  public void testIsNotNull() {
    String sql = "select key, value, ds from src_union_view where key=86 and ds is not null order by ds;";
    String ast = " [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [20 262]] [8 [20 262]] [8 [20 262]]] [24 [447 [354 [20 262] 347] [21 70 [20 262]]]] [54 [159 [20 262]]]]]";
    testGenerateType(sql, ast);
  }

  public void testFullJoinAndCountDistinct() {
    String sql = "select sum(hash(a.key, a.value, b.key, b.value1,  b.value2)) from ( select src1.key as key, count(src1.value) as value from src src1 group by src1.key ) a full outer join ( select src2.key as key, count(distinct(src2.value)) as value1, count(distinct(src2.key)) as value2 from src1 src2 group by src2.key ) b on (a.key = b.key);";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_FULLOUTERJOIN (TOK_SUBQUERY (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src) src1)) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src1) key) key) (TOK_SELEXPR (TOK_FUNCTION count (. (TOK_TABLE_OR_COL src1) value)) value)) (TOK_GROUPBY (. (TOK_TABLE_OR_COL src1) key)))) a) (TOK_SUBQUERY (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src1) src2)) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src2) key) key) (TOK_SELEXPR (TOK_FUNCTIONDI count (. (TOK_TABLE_OR_COL src2) value)) value1) (TOK_SELEXPR (TOK_FUNCTIONDI count (. (TOK_TABLE_OR_COL src2) key)) value2)) (TOK_GROUPBY (. (TOK_TABLE_OR_COL src2) key)))) b) (= (. (TOK_TABLE_OR_COL a) key) (. (TOK_TABLE_OR_COL b) key)))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_FUNCTION sum (TOK_FUNCTION hash (. (TOK_TABLE_OR_COL a) key) (. (TOK_TABLE_OR_COL a) value) (. (TOK_TABLE_OR_COL b) key) (. (TOK_TABLE_OR_COL b) value1) (. (TOK_TABLE_OR_COL b) value2)))))))";
    String type = " [5 [9 [62 [16 [5 [9 [15 [232 262] 262]] [4 [18 [13 158]] [6 [8 [311 [20 262] 262] 262] [8 [21 262 [311 [20 262] 262]] 262]] [52 [311 [20 262] 262]]]] 262] [16 [5 [9 [15 [232 262] 262]] [4 [18 [13 158]] [6 [8 [311 [20 262] 262] 262] [8 [22 262 [311 [20 262] 262]] 262] [8 [22 262 [311 [20 262] 262]] 262]] [52 [311 [20 262] 262]]]] 262] [354 [311 [20 262] 262] [311 [20 262] 262]]]] [4 [18 [13 158]] [6 [8 [21 262 [21 262 [311 [20 262] 262] [311 [20 262] 262] [311 [20 262] 262] [311 [20 262] 262] [311 [20 262] 262]]]]]]]";
    testGenerateType(sql, type);
    super.testGenerateText(sql, ast);
  }

  public void testLeftJoin() {
    String sql = "select sum(hash(a.key,a.value,b.key,b.value)) from myinput1 a left outer join myinput1 b on a.key = b.value;";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_LEFTOUTERJOIN (TOK_TABREF (TOK_TABNAME myinput1) a) (TOK_TABREF (TOK_TABNAME myinput1) b) (= (. (TOK_TABLE_OR_COL a) key) (. (TOK_TABLE_OR_COL b) value)))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_FUNCTION sum (TOK_FUNCTION hash (. (TOK_TABLE_OR_COL a) key) (. (TOK_TABLE_OR_COL a) value) (. (TOK_TABLE_OR_COL b) key) (. (TOK_TABLE_OR_COL b) value)))))))";
    String type = " [5 [9 [60 [15 [232 262] 262] [15 [232 262] 262] [354 [311 [20 262] 262] [311 [20 262] 262]]]] [4 [18 [13 158]] [6 [8 [21 262 [21 262 [311 [20 262] 262] [311 [20 262] 262] [311 [20 262] 262] [311 [20 262] 262]]]]]]]";
    testGenerateType(sql, type);
    super.testGenerateText(sql, ast);
  }

  public void testOrderByDesc() {
    String sql = "select columnshortcuttable.* from columnshortcuttable order by key asc, value desc;";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME columnshortcuttable))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_ALLCOLREF (TOK_TABNAME columnshortcuttable)))) (TOK_ORDERBY (TOK_TABSORTCOLNAMEASC (TOK_TABLE_OR_COL key)) (TOK_TABSORTCOLNAMEDESC (TOK_TABLE_OR_COL value)))))";
    String type = " [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [19 [232 262]]]] [54 [159 [20 262]] [160 [20 262]]]]]";
    testGenerateType(sql, type);
    super.testGenerateText(sql, ast);
  }

  public void test1() {
    String sql = "select one,two from db2.destinTable order by one desc, two desc;";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME db2 destinTable))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL one)) (TOK_SELEXPR (TOK_TABLE_OR_COL two))) (TOK_ORDERBY (TOK_TABSORTCOLNAMEDESC (TOK_TABLE_OR_COL one)) (TOK_TABSORTCOLNAMEDESC (TOK_TABLE_OR_COL two)))))";
    String type = " [5 [9 [15 [232 262 262]]] [4 [18 [13 158]] [6 [8 [20 262]] [8 [20 262]]] [54 [160 [20 262]] [160 [20 262]]]]]";
    testGenerateType(sql, type);
    super.testGenerateText(sql, ast);
  }

  public void testSelectDistinct() {
    String sql = "select count(1) from ( select src.key, src.value from src union all select distinct src.key, src.value from src ) src_12 join (select src.key as k, src.value as v from src) src3 on src_12.key = src3.k and src3.k < 200;";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_JOIN (TOK_SUBQUERY (TOK_UNION (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) key)) (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) value))))) (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECTDI (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) key)) (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) value)))))) src_12) (TOK_SUBQUERY (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) key) k) (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) value) v)))) src3) (and (= (. (TOK_TABLE_OR_COL src_12) key) (. (TOK_TABLE_OR_COL src3) k)) (< (. (TOK_TABLE_OR_COL src3) k) 200)))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_FUNCTION count 1)))))";
    String type = " [5 [9 [59 [16 [58 [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [311 [20 262] 262]] [8 [311 [20 262] 262]]]]] [5 [9 [15 [232 262]]] [4 [18 [13 158]] [7 [8 [311 [20 262] 262]] [8 [311 [20 262] 262]]]]]] 262] [16 [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [311 [20 262] 262] 262] [8 [311 [20 262] 262] 262]]]] 262] [447 [354 [311 [20 262] 262] [311 [20 262] 262]] [379 [311 [20 262] 262] 347]]]] [4 [18 [13 158]] [6 [8 [21 262 347]]]]]";
    testGenerateType(sql, type);
    super.testGenerateText(sql, ast);
  }

   public void testCountDistinct() {
    String sql = "select count(1), count(*), count(a), count(b), count(c), count(d), count(distinct a), count(distinct b), count(distinct c), count(distinct d), count(distinct a,b), count(distinct b,c), count(distinct c,d), count(distinct a,d), count(distinct a,c), count(distinct b,d), count(distinct a,b,c), count(distinct b,c,d), count(distinct a,c,d), count(distinct a,b,d), count(distinct a,b,c,d) from abcd;";
    String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME abcd))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_FUNCTION count 1)) (TOK_SELEXPR (TOK_FUNCTIONSTAR count)) (TOK_SELEXPR (TOK_FUNCTION count (TOK_TABLE_OR_COL a))) (TOK_SELEXPR (TOK_FUNCTION count (TOK_TABLE_OR_COL b))) (TOK_SELEXPR (TOK_FUNCTION count (TOK_TABLE_OR_COL c))) (TOK_SELEXPR (TOK_FUNCTION count (TOK_TABLE_OR_COL d))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL a))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL b))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL c))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL d))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL a) (TOK_TABLE_OR_COL b))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL b) (TOK_TABLE_OR_COL c))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL c) (TOK_TABLE_OR_COL d))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL a) (TOK_TABLE_OR_COL d))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL a) (TOK_TABLE_OR_COL c))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL b) (TOK_TABLE_OR_COL d))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL a) (TOK_TABLE_OR_COL b) (TOK_TABLE_OR_COL c))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL b) (TOK_TABLE_OR_COL c) (TOK_TABLE_OR_COL d))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL a) (TOK_TABLE_OR_COL c) (TOK_TABLE_OR_COL d))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL a) (TOK_TABLE_OR_COL b) (TOK_TABLE_OR_COL d))) (TOK_SELEXPR (TOK_FUNCTIONDI count (TOK_TABLE_OR_COL a) (TOK_TABLE_OR_COL b) (TOK_TABLE_OR_COL c) (TOK_TABLE_OR_COL d))))))";
    String type = " [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [21 262 347]] [8 [23 262]] [8 [21 262 [20 262]]] [8 [21 262 [20 262]]] [8 [21 262 [20 262]]] [8 [21 262 [20 262]]] [8 [22 262 [20 262]]] [8 [22 262 [20 262]]] [8 [22 262 [20 262]]] [8 [22 262 [20 262]]] [8 [22 262 [20 262] [20 262]]] [8 [22 262 [20 262] [20 262]]] [8 [22 262 [20 262] [20 262]]] [8 [22 262 [20 262] [20 262]]] [8 [22 262 [20 262] [20 262]]] [8 [22 262 [20 262] [20 262]]] [8 [22 262 [20 262] [20 262] [20 262]]] [8 [22 262 [20 262] [20 262] [20 262]]] [8 [22 262 [20 262] [20 262] [20 262]]] [8 [22 262 [20 262] [20 262] [20 262]]] [8 [22 262 [20 262] [20 262] [20 262] [20 262]]]]]]";
    testGenerateType(sql, type);
    super.testGenerateText(sql, ast);
  }


   public void testLSquare() {
     String sql = "select dest1.a[0], dest1.b[0], dest1.c['key2'], dest1.d, dest1.e from dest1;";
     String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME dest1))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR ([ (. (TOK_TABLE_OR_COL dest1) a) 0)) (TOK_SELEXPR ([ (. (TOK_TABLE_OR_COL dest1) b) 0)) (TOK_SELEXPR ([ (. (TOK_TABLE_OR_COL dest1) c) 'key2')) (TOK_SELEXPR (. (TOK_TABLE_OR_COL dest1) d)) (TOK_SELEXPR (. (TOK_TABLE_OR_COL dest1) e)))))";
     String type = " [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [430 [311 [20 262] 262] 347]] [8 [430 [311 [20 262] 262] 347]] [8 [430 [311 [20 262] 262] 243]] [8 [311 [20 262] 262]] [8 [311 [20 262] 262]]]]]";
     testGenerateType(sql, type);
     super.testGenerateText(sql, ast);
   }


   public void testIn() {
     String sql = "select * from intable where d in (29098519.0) and f in (1410.0) and i in (996) and l in (40408519555) and s in ('test_string') and t in (12);";
     String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME intable))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR TOK_ALLCOLREF)) (TOK_WHERE (and (and (and (and (and (TOK_FUNCTION in (TOK_TABLE_OR_COL d) 29098519.0) (TOK_FUNCTION in (TOK_TABLE_OR_COL f) 1410.0)) (TOK_FUNCTION in (TOK_TABLE_OR_COL i) 996)) (TOK_FUNCTION in (TOK_TABLE_OR_COL l) 40408519555)) (TOK_FUNCTION in (TOK_TABLE_OR_COL s) 'test_string')) (TOK_FUNCTION in (TOK_TABLE_OR_COL t) 12)))))";
     String type = " [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 19]] [24 [447 [447 [447 [447 [447 [21 278 [20 262] 347] [21 278 [20 262] 347]] [21 278 [20 262] 347]] [21 278 [20 262] 347]] [21 278 [20 262] 243]] [21 278 [20 262] 347]]]]]";
     testGenerateType(sql, type);
     super.testGenerateText(sql, ast);
   }

   public void testAutoJoin27() {
     String sql = "select count(1) from ( select src.key, src.value from src union all select distinct src.key, src.value from src ) src_12 join (select src.key as k, src.value as v from src) src3 on src_12.key = src3.k and src3.k < 200;";
     String ast = "(TOK_QUERY (TOK_FROM (TOK_JOIN (TOK_SUBQUERY (TOK_UNION (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) key)) (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) value))))) (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECTDI (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) key)) (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) value)))))) src_12) (TOK_SUBQUERY (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) key) k) (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) value) v)))) src3) (and (= (. (TOK_TABLE_OR_COL src_12) key) (. (TOK_TABLE_OR_COL src3) k)) (< (. (TOK_TABLE_OR_COL src3) k) 200)))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_FUNCTION count 1)))))";
     String type = " [5 [9 [59 [16 [58 [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [311 [20 262] 262]] [8 [311 [20 262] 262]]]]] [5 [9 [15 [232 262]]] [4 [18 [13 158]] [7 [8 [311 [20 262] 262]] [8 [311 [20 262] 262]]]]]] 262] [16 [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [311 [20 262] 262] 262] [8 [311 [20 262] 262] 262]]]] 262] [447 [354 [311 [20 262] 262] [311 [20 262] 262]] [379 [311 [20 262] 262] 347]]]] [4 [18 [13 158]] [6 [8 [21 262 347]]]]]";
     testGenerateType(sql, type);
     super.testGenerateText(sql, ast);
   }

   public void test2() {
     String sql = "select str_to_map(t,'_','=')['333'] from tbl_s2m;";
     String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME tbl_s2m))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR ([ (TOK_FUNCTION str_to_map (TOK_TABLE_OR_COL t) '_' '=') '333')))))";
     String type = " [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [430 [21 262 [20 262] 243 243] 243]]]]]";
     testGenerateType(sql, type);
     super.testGenerateText(sql, ast);
   }

   public void testUDFTrim() {
     String sql = "select '|', trim(dest1.c1), '|', rtrim(dest1.c1), '|', ltrim(dest1.c1), '|' from dest1;";
     String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME dest1))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR '|') (TOK_SELEXPR (TOK_FUNCTION trim (. (TOK_TABLE_OR_COL dest1) c1))) (TOK_SELEXPR '|') (TOK_SELEXPR (TOK_FUNCTION rtrim (. (TOK_TABLE_OR_COL dest1) c1))) (TOK_SELEXPR '|') (TOK_SELEXPR (TOK_FUNCTION ltrim (. (TOK_TABLE_OR_COL dest1) c1))) (TOK_SELEXPR '|'))))";
     String type = " [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 243] [8 [21 262 [311 [20 262] 262]]] [8 243] [8 [21 262 [311 [20 262] 262]]] [8 243] [8 [21 262 [311 [20 262] 262]]] [8 243]]]]";
     testGenerateType(sql, type);
     super.testGenerateText(sql, ast);
   }

   public void testUnaryMinus() {
     String sql = "select round(ln(3.0),12), ln(0.0), ln(-1), round(log(3.0),12), log(0.0), log(-1), round(log2(3.0),12), log2(0.0), log2(-1), round(log10(3.0),12), log10(0.0), log10(-1), round(log(2, 3.0),12), log(2, 0.0), log(2, -1), log(0.5, 2), log(2, 0.5), round(exp(2.0),12), pow(2,3), power(2,3), power(2,-3), power(0.5, -3), power(4, 0.5), power(-1, 0.5), power(-1, 2) from dest1;";
     String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME dest1))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_FUNCTION round (TOK_FUNCTION ln 3.0) 12)) (TOK_SELEXPR (TOK_FUNCTION ln 0.0)) (TOK_SELEXPR (TOK_FUNCTION ln (- 1))) (TOK_SELEXPR (TOK_FUNCTION round (TOK_FUNCTION log 3.0) 12)) (TOK_SELEXPR (TOK_FUNCTION log 0.0)) (TOK_SELEXPR (TOK_FUNCTION log (- 1))) (TOK_SELEXPR (TOK_FUNCTION round (TOK_FUNCTION log2 3.0) 12)) (TOK_SELEXPR (TOK_FUNCTION log2 0.0)) (TOK_SELEXPR (TOK_FUNCTION log2 (- 1))) (TOK_SELEXPR (TOK_FUNCTION round (TOK_FUNCTION log10 3.0) 12)) (TOK_SELEXPR (TOK_FUNCTION log10 0.0)) (TOK_SELEXPR (TOK_FUNCTION log10 (- 1))) (TOK_SELEXPR (TOK_FUNCTION round (TOK_FUNCTION log 2 3.0) 12)) (TOK_SELEXPR (TOK_FUNCTION log 2 0.0)) (TOK_SELEXPR (TOK_FUNCTION log 2 (- 1))) (TOK_SELEXPR (TOK_FUNCTION log 0.5 2)) (TOK_SELEXPR (TOK_FUNCTION log 2 0.5)) (TOK_SELEXPR (TOK_FUNCTION round (TOK_FUNCTION exp 2.0) 12)) (TOK_SELEXPR (TOK_FUNCTION pow 2 3)) (TOK_SELEXPR (TOK_FUNCTION power 2 3)) (TOK_SELEXPR (TOK_FUNCTION power 2 (- 3))) (TOK_SELEXPR (TOK_FUNCTION power 0.5 (- 3))) (TOK_SELEXPR (TOK_FUNCTION power 4 0.5)) (TOK_SELEXPR (TOK_FUNCTION power (- 1) 0.5)) (TOK_SELEXPR (TOK_FUNCTION power (- 1) 2)))))";
     String type = " [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [21 262 [21 262 347] 347]] [8 [21 262 347]] [8 [21 262 [432 347]]] [8 [21 262 [21 262 347] 347]] [8 [21 262 347]] [8 [21 262 [432 347]]] [8 [21 262 [21 262 347] 347]] [8 [21 262 347]] [8 [21 262 [432 347]]] [8 [21 262 [21 262 347] 347]] [8 [21 262 347]] [8 [21 262 [432 347]]] [8 [21 262 [21 262 347 347] 347]] [8 [21 262 347 347]] [8 [21 262 347 [432 347]]] [8 [21 262 347 347]] [8 [21 262 347 347]] [8 [21 262 [21 262 347] 347]] [8 [21 262 347 347]] [8 [21 262 347 347]] [8 [21 262 347 [432 347]]] [8 [21 262 347 [432 347]]] [8 [21 262 347 347]] [8 [21 262 [432 347] 347]] [8 [21 262 [432 347] 347]]]]]";
     testGenerateType(sql, type);
     super.testGenerateText(sql, ast);
   }


   public void testUDFString() {
     String sql = "select * from (select * from src where value = 'val_66' or value = 'val_8') t where value <> test_udf_get_java_string(\"val_8\");";
     String ast = "(TOK_QUERY (TOK_FROM (TOK_SUBQUERY (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR TOK_ALLCOLREF)) (TOK_WHERE (or (= (TOK_TABLE_OR_COL value) 'val_66') (= (TOK_TABLE_OR_COL value) 'val_8'))))) t)) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR TOK_ALLCOLREF)) (TOK_WHERE (<> (TOK_TABLE_OR_COL value) (TOK_FUNCTION test_udf_get_java_string \"val_8\")))))";
     String type = " [5 [9 [16 [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 19]] [24 [257 [354 [20 262] 243] [354 [20 262] 243]]]]] 262]] [4 [18 [13 158]] [6 [8 19]] [24 [443 [20 262] [21 262 243]]]]]";
     testGenerateType(sql, type);
//     super.testTranslateText(sql, ast); ERROR BECAUSE OF != EITHER <>
   }


   public void testTokNull() {
     String sql = "select col1,col2, field(66,col1), field(66,col1, col2), field(86, col2, col1), field(86, col1, col1), field(86,col1,n,col2), field(NULL,col1,n,col2), field(col1, col2) from (select col1, col2, NULL as n from test_table1 where col1=86 or col1=66) t;";
     String ast = "(TOK_QUERY (TOK_FROM (TOK_SUBQUERY (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME test_table1))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL col1)) (TOK_SELEXPR (TOK_TABLE_OR_COL col2)) (TOK_SELEXPR TOK_NULL n)) (TOK_WHERE (or (= (TOK_TABLE_OR_COL col1) 86) (= (TOK_TABLE_OR_COL col1) 66))))) t)) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL col1)) (TOK_SELEXPR (TOK_TABLE_OR_COL col2)) (TOK_SELEXPR (TOK_FUNCTION field 66 (TOK_TABLE_OR_COL col1))) (TOK_SELEXPR (TOK_FUNCTION field 66 (TOK_TABLE_OR_COL col1) (TOK_TABLE_OR_COL col2))) (TOK_SELEXPR (TOK_FUNCTION field 86 (TOK_TABLE_OR_COL col2) (TOK_TABLE_OR_COL col1))) (TOK_SELEXPR (TOK_FUNCTION field 86 (TOK_TABLE_OR_COL col1) (TOK_TABLE_OR_COL col1))) (TOK_SELEXPR (TOK_FUNCTION field 86 (TOK_TABLE_OR_COL col1) (TOK_TABLE_OR_COL n) (TOK_TABLE_OR_COL col2))) (TOK_SELEXPR (TOK_FUNCTION field TOK_NULL (TOK_TABLE_OR_COL col1) (TOK_TABLE_OR_COL n) (TOK_TABLE_OR_COL col2))) (TOK_SELEXPR (TOK_FUNCTION field (TOK_TABLE_OR_COL col1) (TOK_TABLE_OR_COL col2))))))";
     String type = " [5 [9 [16 [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [20 262]] [8 [20 262]] [8 68 262]] [24 [257 [354 [20 262] 347] [354 [20 262] 347]]]]] 262]] [4 [18 [13 158]] [6 [8 [20 262]] [8 [20 262]] [8 [21 262 347 [20 262]]] [8 [21 262 347 [20 262] [20 262]]] [8 [21 262 347 [20 262] [20 262]]] [8 [21 262 347 [20 262] [20 262]]] [8 [21 262 347 [20 262] [20 262] [20 262]]] [8 [21 262 68 [20 262] [20 262] [20 262]]] [8 [21 262 [20 262] [20 262]]]]]]";
     testGenerateType(sql, type);
     super.testGenerateText(sql, ast);
   }

   public void test3() {
     String sql = "select key, count(1) from ( select '1' as key from src union all select reverse(key) as key from src union all select key as key from src union all select astring as key from src_thrift union all select lstring[0] as key from src_thrift ) union_output group by key;";
     String ast = "(TOK_QUERY (TOK_FROM (TOK_SUBQUERY (TOK_UNION (TOK_UNION (TOK_UNION (TOK_UNION (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR '1' key)))) (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_FUNCTION reverse (TOK_TABLE_OR_COL key)) key))))) (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL key) key))))) (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src_thrift))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL astring) key))))) (TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src_thrift))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR ([ (TOK_TABLE_OR_COL lstring) 0) key))))) union_output)) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL key)) (TOK_SELEXPR (TOK_FUNCTION count 1))) (TOK_GROUPBY (TOK_TABLE_OR_COL key))))";
     String type = " [5 [9 [16 [58 [58 [58 [58 [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 243 262]]]] [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [21 262 [20 262]] 262]]]]] [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [20 262] 262]]]]] [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [20 262] 262]]]]] [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [430 [20 262] 347] 262]]]]] 262]] [4 [18 [13 158]] [6 [8 [20 262]] [8 [21 262 347]]] [52 [20 262]]]]";
     testGenerateType(sql, type);
     super.testGenerateText(sql, ast);
   }

   public void testUDFDot() {
     String sql = "select ngrams(sentences(lower(contents)), 5, 100, 1000).estfrequency from kafka;";
     String ast = "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME kafka))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_FUNCTION ngrams (TOK_FUNCTION sentences (TOK_FUNCTION lower (TOK_TABLE_OR_COL contents))) 5 100 1000) estfrequency)))))";
     String type = " [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [311 [21 262 [21 262 [21 262 [20 262]]] 347 347 347] 262]]]]]";
     testGenerateType(sql, type);
     super.testGenerateText(sql, ast);
   }


  // public void test() {
  // String sql = "select a from t1 where b > all (select b from t2 where t2.c > 100);";
  // String ast =
  // "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME kafka))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (. (TOK_FUNCTION ngrams (TOK_FUNCTION sentences (TOK_FUNCTION lower (TOK_TABLE_OR_COL contents))) 5 100 1000) estfrequency)))))";
  // String type =
  // " [5 [9 [15 [232 262]]] [4 [18 [13 158]] [6 [8 [311 [21 262 [21 262 [21 262 [20 262]]] 347 347 347] 262]]]]]";
  // testGenerateType(sql, type);
  // }

  // public void testPPTSQL() {
  // String sql =
  // "select a, sum(b) from t1 where c!=0 and c!=1 and c > all (select c from t2 where x = 2 or t2.y > t1.d) and c in (select c from t3 where y = 3) or exists (select d from t4 where t4.d = t1.d or t4.e>2 ) group by a having sum(b) >  (select max(bb) from t5 where z != 4);";
  // String ast =
  // "(TOK_QUERY (TOK_FROM (TOK_TABREF (TOK_TABNAME src))) (TOK_INSERT (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE)) (TOK_SELECT (TOK_SELEXPR (TOK_FUNCTION count (TOK_TABLE_OR_COL key)))) (TOK_GROUPBY (TOK_TABLE_OR_COL key))))";
  // testTranslate(sql, ast);
  // }


}
