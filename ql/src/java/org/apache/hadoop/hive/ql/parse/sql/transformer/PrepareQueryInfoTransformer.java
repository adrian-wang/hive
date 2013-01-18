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

import java.util.Stack;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlASTNode;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Build QueryInfo tree for SQL AST tree.
 * PrepareQueryInfoTransformer.
 *
 */
public class PrepareQueryInfoTransformer extends BaseSqlASTTransformer  {

  SqlASTTransformer tf;
  private final SqlXlateUtil.AliasGenerator aliasGen = new SqlXlateUtil.AliasGenerator();

  @Override
  public void transform(SqlASTNode tree, TranslateContext context) throws SqlXlateException {
    this.tf.transformAST(tree, context);
    prepareQueryInfo(tree, context);

  }

  public PrepareQueryInfoTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  void prepareQueryInfo(SqlASTNode tree, TranslateContext context) throws SqlXlateException {
    Stack<Integer> stack = new Stack<Integer>();
    stack.push(-999);//for first peek;
    prepare(tree, null, stack);
    context.setQInfoRoot(tree.getQueryInfo());
  }

  /**
   * Prepare the Facility data structures for later AST generation.
   *
   * @param ast
   *          SQL AST
   * @param qInfo
   *          the current QueryInfo object
   * @throws SqlXlateException
   */
  protected void prepare(SqlASTNode ast, QueryInfo qInfo, Stack<Integer> stack)
      throws SqlXlateException {

    switch (ast.getType()) {
    case PantheraParser_PLSQLParser.STATEMENTS:
      // Prepare the root QueryInfo at SQL AST root node
      qInfo = prepareQInfo(ast, qInfo);
      break;
    case PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT: {
      // Prepare a new QueryInfo for each query (including top level queries and subqueries in from
      // clause)
      // subqueries in filters in where clauses will not have new QInfo
      // created. FilterBlocks will be created for them.
      int nodeType = stack.peek();
      if (nodeType == PantheraParser_PLSQLParser.SQL92_RESERVED_FROM || qInfo.isQInfoTreeRoot()) {
        qInfo = prepareQInfo(ast, qInfo);
      }
      // Prepare the top most Filter Blocks
      break;
    }
    case PantheraParser_PLSQLParser.SQL92_RESERVED_FROM: {
      // Prepare the from subtree
      prepareFrom(ast, qInfo);
      stack.push(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
      break;
    }
    case PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE: {
      stack.push(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
      break;
    }

    case PantheraParser_PLSQLParser.SELECT_LIST: {
      stack.push(PantheraParser_PLSQLParser.SELECT_LIST);
      break;
    }
    case PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING: {
      stack.push(PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING);
      break;
    }
    default:
    }

    // add reference to qInfo ot each Sql AST node
    ast.setQueryInfo(qInfo);
    // if do not skip recursion, iterate all the children
    for (int i = 0; i < ast.getChildCount(); i++) {
      prepare((SqlASTNode) ast.getChild(i), qInfo, stack);
    }

    if (ast.getType() == stack.peek()) {
      stack.pop();
    }
  }

  /**
   * Prepare Query Info object
   *
   * @param ast
   * @param qInfo
   * @return
   */
  private QueryInfo prepareQInfo(SqlASTNode ast, QueryInfo qInfo) {
    QueryInfo nqi = new QueryInfo();
    nqi.setParentQueryInfo(qInfo);
    if (qInfo != null) {
      nqi.setSelectKeyForThisQ(ast);
      qInfo.addChild(nqi);
    }
    return nqi;
  }

  /**
   * Prepare the from clause.
   *
   * @param src
   * @param qInfo
   * @throws SqlXlateException
   */
  private void prepareFrom(SqlASTNode src, QueryInfo qInfo) throws SqlXlateException {
    // set from clause for this query
    qInfo.setFrom(src);
    // get subquery alias in from
    prepareSubQAliases(src, qInfo);
  }

  /**
   * Prepare SubQuery Aliases
   *
   * @param src
   * @param qInfo
   * @throws SqlXlateException
   */
  private void prepareSubQAliases(SqlASTNode src, QueryInfo qInfo) throws SqlXlateException {
    // prepare subq alias for each qInfo
    if (src.getType() == PantheraParser_PLSQLParser.TABLE_REF_ELEMENT) {
      SqlASTNode alias = (SqlASTNode) src.getFirstChildWithType(PantheraParser_PLSQLParser.ALIAS);
      SqlASTNode child2 = null;
      SqlASTNode subquery = null;
      if ((child2 = (SqlASTNode) src
          .getFirstChildWithType(PantheraParser_PLSQLParser.TABLE_EXPRESSION)) != null) {
        if ((child2 = (SqlASTNode) child2
            .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_MODE)) != null) {
          if ((child2 = (SqlASTNode) child2
              .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_STATEMENT)) != null) {
            if ((child2 = (SqlASTNode) child2
                .getFirstChildWithType(PantheraParser_PLSQLParser.SUBQUERY)) != null) {
              subquery = child2;
            }
          }
        }
      }
      if (subquery != null) {
        ASTNode aliasNode = null;
        if (alias == null) {
          aliasNode = SqlXlateUtil.newASTNode(HiveParser.Identifier, aliasGen
              .generateAliasName());
        } else {
          aliasNode = genForAlias(alias);
        }
        qInfo.setSubQAlias(subquery, aliasNode);
      }
    }

    //TODO it's not necessary to travel all tree, just travel top TABLE_REF_ELEMENT is ok.
    for (int i = 0; i < src.getChildCount(); i++) {
      prepareSubQAliases((SqlASTNode) src.getChild(i), qInfo);
    }
  }

  /**
   * Generate Hive AST for Alias node
   *
   * @param src
   * @return
   * @throws SqlXlateException
   */
  private ASTNode genForAlias(SqlASTNode src) throws SqlXlateException {
    String text = src.getChild(0).getText();
    ASTNode alias = SqlXlateUtil.newASTNode(HiveParser.Identifier, text);
    // SqlXlateUtil.attachChild(dest, alias);
    return alias;
  }
}
