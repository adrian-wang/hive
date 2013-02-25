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
package org.apache.hadoop.hive.ql.parse.sql.transformer;

import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.SqlASTNode;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Transform top query by enclosing with select statement.
 * TopQueryTransformer.
 *
 */
public class TopQueryTransformer extends BaseSqlASTTransformer {

  SqlASTTransformer tf;

  public TopQueryTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(SqlASTNode tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    trans(tree, context);

  }

  private void trans(SqlASTNode tree, TranslateContext context) throws SqlXlateException {
    CommonTree selectStatement = (CommonTree) tree
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_STATEMENT);
    if (selectStatement != null) {
      CommonTree subquery = (CommonTree) selectStatement
          .getFirstChildWithType(PantheraParser_PLSQLParser.SUBQUERY);
      if (subquery != null && subquery.getChildIndex() == 0) {
        if (subquery.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_UNION) != null) {
          makeTop(selectStatement, context);
        }
      }
    }
  }

  private void makeTop(CommonTree topSelectStatement, TranslateContext context)
      throws SqlXlateException {


    // create top select
    CommonTree topSubquery = FilterBlockUtil.createSqlASTNode(PantheraParser_PLSQLParser.SUBQUERY,
        "SUBQUERY");
    CommonTree topSelect = FilterBlockUtil.createSqlASTNode(
        PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    FilterBlockUtil.attachChild(topSubquery, topSelect);
    CommonTree from = FilterBlockUtil.createSqlASTNode(
        PantheraParser_PLSQLParser.SQL92_RESERVED_FROM, "from");
    FilterBlockUtil.attachChild(topSelect, from);
    CommonTree tableRef = FilterBlockUtil.createSqlASTNode(PantheraParser_PLSQLParser.TABLE_REF,
        "TABLE_REF");
    FilterBlockUtil.attachChild(from, tableRef);
    CommonTree tableRefElement = FilterBlockUtil.createSqlASTNode(
        PantheraParser_PLSQLParser.TABLE_REF_ELEMENT, "TABLE_REF_ELEMENT");
    CommonTree alias = FilterBlockUtil.createAlias(context);
    FilterBlockUtil.attachChild(tableRefElement, alias);
    FilterBlockUtil.attachChild(tableRef, tableRefElement);
    CommonTree tableExpression = FilterBlockUtil.createSqlASTNode(
        PantheraParser_PLSQLParser.TABLE_EXPRESSION,
        "TABLE_EXPRESSION");
    FilterBlockUtil.attachChild(tableRefElement, tableExpression);
    CommonTree selectMode = FilterBlockUtil.createSqlASTNode(
        PantheraParser_PLSQLParser.SELECT_MODE,
        "SELECT_MODE");
    FilterBlockUtil.attachChild(tableExpression, selectMode);
    CommonTree selectStatement = FilterBlockUtil.createSqlASTNode(
        PantheraParser_PLSQLParser.SELECT_STATEMENT,
        "SELECT_STATEMENT");
    FilterBlockUtil.attachChild(selectMode, selectStatement);

    CommonTree subquery = (CommonTree) topSelectStatement.deleteChild(0);
    FilterBlockUtil.attachChild(selectStatement, subquery);
    SqlXlateUtil.addCommonTreeChild(topSelectStatement, 0, topSubquery);

    topSelect.addChild(FilterBlockUtil.createSqlASTNode(PantheraParser_PLSQLParser.ASTERISK, "*"));
  }

}
