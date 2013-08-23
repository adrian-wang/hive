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
package org.apache.hadoop.hive.ql.parse.sql.transformer.fb;

import java.util.List;
import java.util.Stack;

import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.SqlASTNode;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo.Column;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;


/**
 * create PLSQL filter block.<br>
 * Define filter block's PLSQL node type.<br>
 * Implement PLSQL node type method.<br>
 * PLSQLFilterBlockFactory.
 *
 */
public class PLSQLFilterBlockFactory extends FilterBlockFactory {

  private static PLSQLFilterBlockFactory instance = new PLSQLFilterBlockFactory();

  private PLSQLFilterBlockFactory() {
    typeMap.put(PantheraParser_PLSQLParser.EQUALS_OP, EQUALS);
    typeMap.put(PantheraParser_PLSQLParser.NOT_EQUAL_OP, NOT_EQUAL);
    typeMap.put(PantheraParser_PLSQLParser.GREATER_THAN_OP, GREATER_THAN);
    typeMap.put(PantheraParser_PLSQLParser.LESS_THAN_OP, LESS_THAN);
    typeMap.put(PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP, LESS_THAN_OR_EQUALS);
    typeMap.put(PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP, GREATER_THAN_OR_EQUALS);
    typeMap.put(PantheraParser_PLSQLParser.NOT_IN, NOT_IN);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_IN, IN);
    typeMap.put(PantheraParser_PLSQLParser.NOT_BETWEEN, NOT_BETWEEN);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_BETWEEN, BETWEEN);
    typeMap.put(PantheraParser_PLSQLParser.NOT_LIKE, NOT_LIKE);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_LIKE, LIKE);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_AND, AND);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_OR, OR);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_NOT, NOT);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, SELECT);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_EXISTS, EXISTS);
    typeMap.put(PantheraParser_PLSQLParser.SELECT_LIST, SELECT_LIST);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE, WHERE);
    typeMap.put(PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING, HAVING);
    typeMap.put(PantheraParser_PLSQLParser.IS_NULL, IS_NULL);
    typeMap.put(PantheraParser_PLSQLParser.IS_NOT_NULL, IS_NOT_NULL);
  }

  public static PLSQLFilterBlockFactory getInstance() {
    return instance;
  }

  /**
   *
   */
  @Override
  public boolean isCorrelated(QueryInfo qInfo, Stack<CommonTree> selectStack, CommonTree branch)
      throws SqlXlateException {
    if (branch.getType() != PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
      branch = FilterBlockUtil.findOnlyNode(branch, PantheraParser_PLSQLParser.CASCATED_ELEMENT);
      if (branch == null) {
        return false;
      }
    }
    CommonTree child = (CommonTree) branch.getChild(0);
    if (child.getType() == PantheraParser_PLSQLParser.ANY_ELEMENT) {
      if (selectStack.size() <= 1) {
        return false;
      }
      if (child.getChildCount() == 2) {// tableName.columnName

        if (SqlXlateUtil.containTableName(child.getChild(0).getText(), (CommonTree) selectStack
            .peek().getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM))) {
          return false;
        }
        CommonTree temp = selectStack.pop();
        boolean correlated = SqlXlateUtil.containTableName(child.getChild(0).getText(),
            (CommonTree) selectStack.peek().getFirstChildWithType(
                PantheraParser_PLSQLParser.SQL92_RESERVED_FROM));
        selectStack.push(temp);
        if (correlated) {
          return true;
        }
        throw new SqlXlateException((CommonTree) child.getChild(0), "Correlated level is more than 2");
      } else {// only columnName
        String columnName = child.getChild(0).getText();
        CommonTree bottomSelect = selectStack.pop();
        CommonTree topSelect = selectStack.peek();
        selectStack.push(bottomSelect);
        CommonTree bottomFrom = (CommonTree) bottomSelect
            .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
        CommonTree topFrom = (CommonTree) topSelect
            .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
        List<Column> bottomColumnList = qInfo.getRowInfo(bottomFrom);
        for (Column column : bottomColumnList) {
          if (columnName.equals(column.getColAlias())) {
            return false;
          }
        }
        List<Column> topColumnList = qInfo.getRowInfo(topFrom);
        for (Column column : topColumnList) {
          if (columnName.equals(column.getColAlias())) {
            return true;
          }
        }
        throw new SqlXlateException((CommonTree) child.getChild(0), "Correlated level is more than 2");
      }
    }

    return false;
  }


  /**
   * FIXME process the case without table name but correlated.
   *
   * @deprecated
   */
  @Deprecated
  public boolean isCorrelated_TBD(Stack<CommonTree> selectStack, CommonTree branch)
      throws SqlXlateException {
    if (branch.getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
      SqlASTNode child = (SqlASTNode) branch.getChild(0);
      if (child.getType() == PantheraParser_PLSQLParser.ANY_ELEMENT) {
        if (child.getChildCount() == 2) {// tableName.columnName
          if (selectStack.size() <= 1) {
            return false;
          }
          if (SqlXlateUtil.containTableName(child.getChild(0).getText(), (CommonTree) selectStack
              .peek().getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM))) {
            return false;
          }
          CommonTree temp = selectStack.pop();
          boolean correlated = SqlXlateUtil.containTableName(child.getChild(0).getText(),
              (CommonTree) selectStack.peek().getFirstChildWithType(
                  PantheraParser_PLSQLParser.SQL92_RESERVED_FROM));
          selectStack.push(temp);
          if (correlated) {
            return true;
          }
          //FIXME here add appropriate node
          throw new SqlXlateException(null, "Correlated level is more than 2");
        }
      }
    }
    // FIXME
    return false;
  }
}
