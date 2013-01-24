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
package org.apache.hadoop.hive.ql.parse.sql.transformer.fb.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.antlr33.runtime.CommonToken;
import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.PLSQLFilterBlockFactory;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.QueryBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.SubQFilterBlock;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;


public abstract class BaseFilterBlockProcessor implements FilterBlockProcessor {

  QueryBlock bottomQuery;
  QueryBlock topQuery;
  SubQFilterBlock subQ;
  CommonTree topSelect;
  CommonTree bottomSelect;
  CommonTree subQNode;
  FilterBlockContext fbContext;
  FilterBlock fb;// current normalFilterBlock
  TranslateContext context;
  Map<String, String> tableAliasMap = new HashMap<String, String>();// <tableName,tableAlias>
  // <tableAlias,<columnNam,columnAliase>> TODO how to do duplicated columnName?
  Map<String, Map<String, String>> columnAliasMap = new HashMap<String, Map<String, String>>();

  /**
   * template method
   */
  @Override
  public void process(FilterBlockContext fbContext, FilterBlock fb, TranslateContext context)
      throws SqlXlateException {
    bottomQuery = fbContext.getQueryStack().pop();
    bottomQuery.setAggregationList(null);// TODO naive
    topQuery = fbContext.getQueryStack().peek();
    fbContext.getQueryStack().push(bottomQuery);
    subQ = fbContext.getSubQStack().peek();
    topSelect = topQuery.cloneSimpleQuery();
    bottomSelect = bottomQuery.cloneWholeQuery();
    if (fb.getTransformedNode() != null) {// for multi-level sub query
      bottomSelect = fb.getTransformedNode();
    }
    subQNode = subQ.getASTNode();
    this.fbContext = fbContext;
    this.fb = fb;
    this.context = context;
    processFB();
    fb.setTransformedNode(topSelect);
  }

  abstract void processFB() throws SqlXlateException;

  void attachChild(CommonTree parent, CommonTree child) {
    if (parent != null) {
      parent.addChild(child);
      if (child != null) {
        child.setParent(parent);
      }
    }
  }

  CommonTree createSqlASTNode(int type, String text) {
    return new CommonTree(new CommonToken(type, text));
  }

  CommonTree createAlias() {
    CommonTree alias = createSqlASTNode(PantheraParser_PLSQLParser.ALIAS, "ALIAS");
    CommonTree aliasName = createSqlASTNode(PantheraParser_PLSQLParser.ID, context.getAliasGen()
        .generateAliasName());
    this.attachChild(alias, aliasName);
    return alias;
  }

  /**
   * Create TABLE_REF_ELEMENT & attach select node to it.
   *
   * @param select
   * @return alias
   */
  CommonTree createTableRefElement(CommonTree select) {

    CommonTree tableRefElement = this.createSqlASTNode(
        PantheraParser_PLSQLParser.TABLE_REF_ELEMENT, "TABLE_REF_ELEMENT");
    CommonTree viewAlias = this.createAlias();
    this.buildTableAliasMap(viewAlias.getChild(0).getText(), FilterBlockUtil.getTableName(select));
    this.attachChild(tableRefElement, viewAlias);
    CommonTree tableExpression = this.createSqlASTNode(PantheraParser_PLSQLParser.TABLE_EXPRESSION,
        "TABLE_EXPRESSION");
    this.attachChild(tableRefElement, tableExpression);
    CommonTree selectMode = this.createSqlASTNode(PantheraParser_PLSQLParser.SELECT_MODE,
        "SELECT_MODE");
    this.attachChild(tableExpression, selectMode);
    CommonTree selectStatement = this.createSqlASTNode(PantheraParser_PLSQLParser.SELECT_STATEMENT,
        "SELECT_STATEMENT");
    this.attachChild(selectMode, selectStatement);
    CommonTree subQuery = this.createSqlASTNode(PantheraParser_PLSQLParser.SUBQUERY, "SUBQUERY");
    this.attachChild(selectStatement, subQuery);
    this.attachChild(subQuery, select);
    return tableRefElement;
  }

  CommonTree createFunction(String functionName, CommonTree element) {
    CommonTree standardFunction = this.createSqlASTNode(
        PantheraParser_PLSQLParser.STANDARD_FUNCTION, "STANDARD_FUNCTION");
    CommonTree function = this.createSqlASTNode(PantheraParser_PLSQLParser.ID, functionName);
    this.attachChild(standardFunction, function);
    CommonTree arguments = this.createSqlASTNode(PantheraParser_PLSQLParser.ARGUMENTS, "ARGUMENTS");
    this.attachChild(function, arguments);
    CommonTree argument = this.createSqlASTNode(PantheraParser_PLSQLParser.ARGUMENT, "ARGUMENT");
    this.attachChild(arguments, argument);
    CommonTree expr = this.createSqlASTNode(PantheraParser_PLSQLParser.EXPR, "EXPR");
    this.attachChild(argument, expr);
    this.attachChild(expr, element);
    return standardFunction;
  }

  CommonTree createOpBranch(CommonTree viewAlias, CommonTree colAlias) {
    CommonTree subQ = FilterBlockUtil.cloneTree(subQNode);
    for (int i = 0; i < subQ.getChildCount(); i++) {
      if (subQ.getChild(i).getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
        continue;
      }
      subQ.getChildren().set(
          i,
          this.createCascatedElement((CommonTree) viewAlias.getChild(0), (CommonTree) colAlias
              .getChild(0)));
    }
    return subQ;
  }

  void buildWhereBranch(CommonTree viewAlias, CommonTree colAlias) {
    CommonTree where = this.createSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE,
        "where");
    CommonTree logicExpr = this.createSqlASTNode(PantheraParser_PLSQLParser.LOGIC_EXPR,
        "LOGIC_EXPR");
    this.attachChild(where, logicExpr);
    this.attachChild(logicExpr, this.createOpBranch(viewAlias, colAlias));
    this.attachChild(topSelect, where);
  }

  /**
   * create join node & attach to tree
   */
  CommonTree createJoin(CommonTree select) {
    CommonTree from = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    CommonTree join = this.createSqlASTNode(PantheraParser_PLSQLParser.JOIN_DEF, "join");
    this.attachChild((CommonTree) from.getChild(0), join);
    return join;
  }

  /**
   * build join node's children without process detail.
   *
   * @param joinTypeNode
   *          join type
   * @return join sub query alias
   */
  CommonTree buildJoin(CommonTree joinTypeNode, CommonTree join, CommonTree select) {
    if (joinTypeNode != null) {
      this.attachChild(join, joinTypeNode);
    }
    CommonTree tableRefElement = this.createTableRefElement(select);
    this.attachChild(join, tableRefElement);

    return (CommonTree) tableRefElement.getFirstChildWithType(PantheraParser_PLSQLParser.ALIAS);
  }

  /**
   * check bottom select's from table alias. If no alias, create it.
   * FIXME if from join, it's wrong.
   *
   * @return
   */
  @Deprecated
  CommonTree checkFromAlias() throws SqlXlateException {
    CommonTree from = (CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    if (from.getChild(0).getChildCount() > 1) {
      throw new SqlXlateException("FIXME if from join, it's wrong.");
    }
    CommonTree tableRefElement = (CommonTree) from.getChild(0).getChild(0);
    if (tableRefElement.getChildCount() > 1) {
      return (CommonTree) tableRefElement.getChild(0);
    }
    CommonTree alias = this.createAlias();
    tableRefElement.getChildren().add(0, alias);
    return alias;

  }

  /**
   * extract join key from filter op node.
   * TODO optimize it.
   *
   * @return false: bottom keys<br>
   *         true: top key
   * @throws SqlXlateException
   */
  Map<Boolean, List<CommonTree>> getFilterKey(CommonTree filterOp) throws SqlXlateException {
    Map<Boolean, List<CommonTree>> result = new HashMap<Boolean, List<CommonTree>>();
    Stack<CommonTree> selectStack = new Stack<CommonTree>();
    selectStack.push(topSelect);
    selectStack.push(bottomSelect);
    for (int i = 0; i < filterOp.getChildCount(); i++) {
      CommonTree child = (CommonTree) filterOp.getChild(i);
      if (!PLSQLFilterBlockFactory.getInstance().isCorrelated(selectStack, child)) {
        if (child.getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
          List<CommonTree> uncorrelatedList = result.get(false);
          if (uncorrelatedList == null) {
            uncorrelatedList = new ArrayList<CommonTree>();
            result.put(false, uncorrelatedList);
          }
          uncorrelatedList.add(child);
        }
      }

      if (PLSQLFilterBlockFactory.getInstance().isCorrelated(selectStack, child)) {
        List<CommonTree> correlatedList = result.get(true);
        if (correlatedList == null) {
          correlatedList = new ArrayList<CommonTree>();
          result.put(true, correlatedList);
        }
        correlatedList.add(child);
      }
    }

    return result;
  }

  /**
   *
   * @return
   * @throws SqlXlateException
   */
  List<Map<Boolean, List<CommonTree>>> getFilterkey() throws SqlXlateException {
    List<Map<Boolean, List<CommonTree>>> result = new ArrayList<Map<Boolean, List<CommonTree>>>();
    this.getWhereKey(fb.getASTNode(), result);
    return result;
  }

  private void getWhereKey(CommonTree filterOp, List<Map<Boolean, List<CommonTree>>> list)
      throws SqlXlateException {
    if (isFilterOp(filterOp)) {
      list.add(getFilterKey(filterOp));
      return;
    } else if (isLogicOp(filterOp)) {
      for (int i = 0; i < filterOp.getChildCount(); i++) {
        getWhereKey((CommonTree) filterOp.getChild(i), list);
      }
    } else {
      throw new SqlXlateException("unknow filter operation:" + filterOp.getText());
    }

  }

  private boolean isFilterOp(CommonTree node) {
    int type = node.getType();
    switch (type) {
    case PantheraParser_PLSQLParser.EQUALS_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP:
    case PantheraParser_PLSQLParser.NOT_EQUAL_OP:
      return true;
    default:
      return false;
    }
  }

  private boolean isLogicOp(CommonTree node) {
    int type = node.getType();
    switch (type) {
    case PantheraParser_PLSQLParser.SQL92_RESERVED_AND:
    case PantheraParser_PLSQLParser.SQL92_RESERVED_OR:
      return true;
    default:
      return false;
    }
  }

  /**
   * add cascatedElement(standardFunction...) branch to bottom SELECT_LIST node
   *
   * @param cascatedElement
   * @return alias
   */
  CommonTree addSelectItem(CommonTree selectList, CommonTree cascatedElement) {
    CommonTree selectItem = this.createSqlASTNode(PantheraParser_PLSQLParser.SELECT_ITEM,
        "SELECT_ITEM");
    this.attachChild(selectList, selectItem);
    CommonTree expr = this.createSqlASTNode(PantheraParser_PLSQLParser.EXPR, "EXPR");
    this.attachChild(selectItem, expr);
    this.attachChild(expr, cascatedElement);
    return this.addAlias(selectItem);
  }

  CommonTree addAlias(CommonTree node) {
    CommonTree alias;
    alias = (CommonTree) node.getFirstChildWithType(PantheraParser_PLSQLParser.ALIAS);
    if (alias == null) {
      alias = this.createAlias();
      this.attachChild(node, alias);
    }
    return alias;
  }


  void buildGroup(CommonTree cascatedElement) {
    CommonTree group = (CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
    if (group == null) {
      group = this.createSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP, "group");
      this.attachChild(bottomSelect, group);
    }
    CommonTree groupByElement = this.createSqlASTNode(PantheraParser_PLSQLParser.GROUP_BY_ELEMENT,
        "GROUP_BY_ELEMENT");
    this.attachChild(group, groupByElement);
    CommonTree expr = this.createSqlASTNode(PantheraParser_PLSQLParser.EXPR, "EXPR");
    this.attachChild(groupByElement, expr);
    this.attachChild(expr, cascatedElement);
  }



  /**
   * build on branch
   *
   * @param op
   * @param child0
   * @param child1
   * @return on node
   */
  CommonTree buildOn(CommonTree op, CommonTree child0, CommonTree child1) {
    CommonTree on = this.createSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_ON, "on");
    this.attachChild(on, this.createLogicExpr(op, child0, child1));
    return on;
  }

  CommonTree buildWhere(CommonTree op, CommonTree child0, CommonTree child1) {
    CommonTree where = this.createSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE,
        "where");
    this.attachChild(where, this.createLogicExpr(op, child0, child1));
    return where;
  }

  CommonTree createLogicExpr(CommonTree op, CommonTree child0, CommonTree child1) {
    CommonTree logicExpr = this.createSqlASTNode(PantheraParser_PLSQLParser.LOGIC_EXPR,
        "LOGIC_EXPR");
    this.attachChild(logicExpr, op);
    this.attachChild(op, child0);
    this.attachChild(op, child1);
    return logicExpr;
  }

  void addConditionToWhere(CommonTree where, CommonTree op, CommonTree child0, CommonTree child1) {
    CommonTree logicExpr = (CommonTree) where
        .getFirstChildWithType(PantheraParser_PLSQLParser.LOGIC_EXPR);
    assert (logicExpr.getChildCount() == 1);
    CommonTree current = (CommonTree) logicExpr.deleteChild(0);
    CommonTree and = this.createSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_AND, "and");
    this.attachChild(logicExpr, and);
    this.attachChild(and, current);
    this.attachChild(op, child0);
    this.attachChild(op, child1);
    this.attachChild(and, op);
  }


  CommonTree deleteBranch(CommonTree root, int branchType) {
    for (int i = 0; i < root.getChildCount(); i++) {
      if (root.getChild(i).getType() == branchType) {
        return (CommonTree) root.deleteChild(i);

      }
    }
    return null;
  }

  /**
   * add alias for every column & build column alias map
   *
   * @param alias
   *          table alias node
   * @param selectList
   * @return
   */
  List<CommonTree> buildSelectListAlias(CommonTree alias, CommonTree selectList) {
    List<CommonTree> aliasList = new ArrayList<CommonTree>();
    String aliasName = alias == null ? "" : alias.getChild(0).getText();
    Map<String, String> columnMap = this.columnAliasMap.get(aliasName);
    if (columnMap == null) {
      columnMap = new HashMap<String, String>();
      this.columnAliasMap.put(aliasName, columnMap);
    }
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      CommonTree columnAlias;
      if (selectItem.getChildCount() > 1) {// had alias
        columnAlias = (CommonTree) selectItem.getChild(1);
      } else {
        columnAlias = this.addAlias(selectItem);
      }
      aliasList.add(columnAlias);
      CommonTree anyElement = (CommonTree) selectItem.getChild(0).getChild(0).getChild(0);
      String columnName;
      if (anyElement.getChildCount() == 2) {
        columnName = anyElement.getChild(1).getText();
      } else {
        columnName = anyElement.getChild(0).getText();
      }
      String columnAliasName = columnAlias.getChild(0).getText();
      columnMap.put(columnName, columnAliasName);
    }
    this.rebuildGroupOrder(alias);
    return aliasList;
  }

  /**
   * clone subQFilterBlock's non-sub-query branch
   *
   * @return
   */
  CommonTree cloneSubQOpElement() {
    for (int i = 0; i < subQNode.getChildCount(); i++) {
      if (subQNode.getChild(i).getType() != PantheraParser_PLSQLParser.SUBQUERY) {
        return FilterBlockUtil.cloneTree((CommonTree) subQNode.getChild(i));
      }
    }
    return null;
  }

  CommonTree createClosingSelect(CommonTree tebleRefElement) {
    CommonTree select = this.createSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT,
        "select");
    CommonTree from = this.createSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM, "from");
    this.attachChild(select, from);
    CommonTree tableRef = this.createSqlASTNode(PantheraParser_PLSQLParser.TABLE_REF, "TABLE_REF");
    this.attachChild(from, tableRef);
    this.attachChild(tableRef, tebleRefElement);
    return select;
  }

  CommonTree createCascatedElement(CommonTree child) {
    CommonTree cascatedElement = this.createSqlASTNode(PantheraParser_PLSQLParser.CASCATED_ELEMENT,
        "CASCATED_ELEMENT");
    CommonTree anyElement = this.createSqlASTNode(PantheraParser_PLSQLParser.ANY_ELEMENT,
        "ANY_ELEMENT");
    this.attachChild(cascatedElement, anyElement);
    this.attachChild(anyElement, child);
    return cascatedElement;
  }

  /**
   * TODO duplicated with below: CommonTree createCascatedElement(CommonTree child1, CommonTree
   * child2
   *
   * @param tableName
   * @param child
   * @return
   */
  CommonTree createCascatedElementWithTableName(CommonTree tableName, CommonTree child) {
    CommonTree cascatedElement = this.createSqlASTNode(PantheraParser_PLSQLParser.CASCATED_ELEMENT,
        "CASCATED_ELEMENT");
    CommonTree anyElement = this.createSqlASTNode(PantheraParser_PLSQLParser.ANY_ELEMENT,
        "ANY_ELEMENT");
    this.attachChild(cascatedElement, anyElement);
    this.attachChild(anyElement, tableName);
    this.attachChild(anyElement, child);
    return cascatedElement;
  }

  CommonTree createCascatedElement(CommonTree child1, CommonTree child2) {
    CommonTree cascatedElement = this.createSqlASTNode(PantheraParser_PLSQLParser.CASCATED_ELEMENT,
        "CASCATED_ELEMENT");
    CommonTree anyElement = this.createSqlASTNode(PantheraParser_PLSQLParser.ANY_ELEMENT,
        "ANY_ELEMENT");
    this.attachChild(cascatedElement, anyElement);
    this.attachChild(anyElement, child1);
    this.attachChild(anyElement, child2);
    return cascatedElement;
  }

  CommonTree createSelectListForClosingSelect(List<CommonTree> aliasList) {
    CommonTree selectList = this.createSqlASTNode(PantheraParser_PLSQLParser.SELECT_LIST,
        "SELECT_LIST");
    for (CommonTree alias : aliasList) {
      CommonTree newAlias = this.addSelectItem(selectList, this
          .createCascatedElement((CommonTree) alias.getChild(0)));
      this.reRebuildGroupOrder(alias, newAlias);
    }
    return selectList;
  }

  /**
   * rebuild select list of bottom select to collect_set function.
   */
  void rebuildCollectSet() {
    CommonTree expr = (CommonTree) bottomSelect.getFirstChildWithType(
        PantheraParser_PLSQLParser.SELECT_LIST).getChild(0).getChild(0);
    CommonTree element = (CommonTree) expr.deleteChild(0);
    CommonTree cascatedElement = this.createSqlASTNode(PantheraParser_PLSQLParser.CASCATED_ELEMENT,
        "CASCATED_ELEMENT");
    this.attachChild(expr, cascatedElement);
    CommonTree routineCall = this.createSqlASTNode(PantheraParser_PLSQLParser.ROUTINE_CALL,
        "ROUTINE_CALL");
    this.attachChild(cascatedElement, routineCall);
    CommonTree routineName = this.createSqlASTNode(PantheraParser_PLSQLParser.ROUTINE_NAME,
        "ROUTINE_NAME");
    this.attachChild(routineCall, routineName);
    CommonTree collectSet = this.createSqlASTNode(PantheraParser_PLSQLParser.ID, "collect_set");
    this.attachChild(routineName, collectSet);
    CommonTree arguments = this.createSqlASTNode(PantheraParser_PLSQLParser.ARGUMENTS, "ARGUMENTS");
    this.attachChild(routineCall, arguments);
    CommonTree arguement = this.createSqlASTNode(PantheraParser_PLSQLParser.ARGUMENT, "ARGUMENT");
    this.attachChild(arguments, arguement);
    CommonTree newExpr = this.createSqlASTNode(PantheraParser_PLSQLParser.EXPR, "EXPR");
    this.attachChild(arguement, newExpr);
    this.attachChild(newExpr, element);
  }

  /**
   * rebuild LOGIC_EXPR branch to array_contains function.
   *
   * @param logicExpr
   * @throws SqlXlateException
   */
  void rebuildArrayContains(CommonTree logicExpr) throws SqlXlateException {
    CommonTree op = (CommonTree) logicExpr.deleteChild(0);
    CommonTree newOp;
    switch (op.getType()) {
    case PantheraParser_PLSQLParser.NOT_IN:
      newOp = this.createSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_NOT, "not");
      this.attachChild(logicExpr, newOp);
      break;
    default:
      throw new SqlXlateException("UnProcess logic operator." + op.getText());
    }
    CommonTree cascatedElement = this.createSqlASTNode(PantheraParser_PLSQLParser.CASCATED_ELEMENT,
        "CASCATED_ELEMENT");
    this.attachChild(newOp, cascatedElement);
    CommonTree routineCall = this.createSqlASTNode(PantheraParser_PLSQLParser.ROUTINE_CALL,
        "ROUTINE_CALL");
    this.attachChild(cascatedElement, routineCall);
    CommonTree routineName = this.createSqlASTNode(PantheraParser_PLSQLParser.ROUTINE_NAME,
        "ROUTINE_NAME");
    this.attachChild(routineCall, routineName);
    CommonTree arrayContains = this.createSqlASTNode(PantheraParser_PLSQLParser.ID,
        "array_contains");
    this.attachChild(routineName, arrayContains);
    CommonTree arguments = this.createSqlASTNode(PantheraParser_PLSQLParser.ARGUMENTS, "ARGUMENTS");
    this.attachChild(routineCall, arguments);
    for (int i = 0; i < op.getChildCount(); i++) {
      CommonTree arguement = this.createSqlASTNode(PantheraParser_PLSQLParser.ARGUMENT, "ARGUMENT");
      this.attachChild(arguments, arguement);
      CommonTree expr = this.createSqlASTNode(PantheraParser_PLSQLParser.EXPR, "EXPR");
      this.attachChild(arguement, expr);
      CommonTree element = (CommonTree) op.getChild(i);
      this.attachChild(expr, element);

    }
  }

  CommonTree createMinus(CommonTree select) {
    CommonTree minus = this.createSqlASTNode(PantheraParser_PLSQLParser.MINUS_SIGN, "minus");
    CommonTree subq = this.createSqlASTNode(PantheraParser_PLSQLParser.SUBQUERY, "SUBQUERY");
    this.attachChild(minus, subq);
    this.attachChild(subq, select);
    return minus;
  }

  private void buildTableAliasMap(String alias, Set<String> tableNames) {
    for (String tableName : tableNames) {
      this.tableAliasMap.put(tableName, alias);
    }
  }

  CommonTree rebuildCascatedElement(CommonTree cascatedElement) {
    CommonTree anyElement = (CommonTree) cascatedElement.getChild(0);
    if (anyElement.getChildCount() <= 1) {
      return cascatedElement;
    }
    CommonTree tableName = (CommonTree) anyElement.getChild(0);
    CommonTree columnName = (CommonTree) anyElement.getChild(1);
    String tableAlias = this.tableAliasMap.get(tableName.getText());
    if (tableAlias != null) {
      tableName.getToken().setText(tableAlias);
      Map<String, String> columnMap = this.columnAliasMap.get(tableAlias);
      if (columnMap != null) {
        String columnAlias = columnMap.get(columnName.getText());
        if (columnAlias != null) {
          columnName.getToken().setText(columnAlias);
        }
      }
    }
    return cascatedElement;
  }

  /**
   * only for bottomSelect
   *
   * @param select
   */
  void processSelectAsterisk(CommonTree select) {
    for (int i = 0; i < select.getChildCount(); i++) {
      CommonTree node = (CommonTree) select.getChild(i);
      if (node.getType() == PantheraParser_PLSQLParser.ASTERISK) {
        node.getToken().setType(PantheraParser_PLSQLParser.SELECT_LIST);
        node.getToken().setText("SELECT_LIST");
      }
    }
  }

  void rebuildSelectListByFilter(boolean needGroup, CommonTree joinSubAlias, CommonTree topAlias)
      throws SqlXlateException {
    List<Map<Boolean, List<CommonTree>>> joinKeys = this.getFilterkey();
    Set<String> selectKeySet = new HashSet<String>();
    for (int i = 0; i < joinKeys.size(); i++) {
      List<CommonTree> bottomKeys = joinKeys.get(i).get(false);
      List<CommonTree> topKeys = joinKeys.get(i).get(true);
      if (bottomKeys != null) {
        for (CommonTree bottomKey : bottomKeys) {
          String selectKey;
          assert (bottomKey.getChildCount() == 2);
          selectKey = bottomKey.getChild(0).getChild(0).getText()
              + bottomKey.getChild(0).getChild(1).getText();// tableName+columnName
          if (needGroup && !selectKeySet.contains(selectKey)) {
            selectKeySet.add(selectKey);
            // group
            this.buildGroup(FilterBlockUtil.cloneTree(bottomKey));
          }
          // add select item
          CommonTree joinKeyAlias = this.addSelectItem((CommonTree) bottomSelect
                .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), FilterBlockUtil
                .cloneTree(bottomKey));
          // modify filter to alias
          CommonTree anyElement = (CommonTree) bottomKey.getChild(0);
          if (anyElement.getChildCount() == 2) {
            ((CommonTree) anyElement.getChild(0)).getToken().setText(
                  joinSubAlias.getChild(0).getText());
            ((CommonTree) anyElement.getChild(1)).getToken().setText(
                  joinKeyAlias.getChild(0).getText());
          }

        }
      }

      if (topKeys != null && topAlias != null) {
        CommonTree topKey = topKeys.get(0);// can't more than one topkey.

        // add select item
        CommonTree joinKeyAlias = this.addSelectItem((CommonTree) topSelect
              .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), FilterBlockUtil
              .cloneTree(topKey));
        // modify filter to alias
        CommonTree anyElement = (CommonTree) topKey.getChild(0);
        if (anyElement.getChildCount() == 2) {
          ((CommonTree) anyElement.getChild(0)).getToken().setText(
                topAlias.getChild(0).getText());
          ((CommonTree) anyElement.getChild(1)).getToken().setText(
                joinKeyAlias.getChild(0).getText());
        }

        // ((CommonTree) anyElement.getChild(0)).getToken().setText(
        // topAlias.getChild(0).getText());
        // Map<String, String> columnMap = columnAliasMap.get(topAlias.getChild(0).getText());
        // String alias = columnMap.get(anyElement.getChild(1).getText());
        // ((CommonTree) anyElement.getChild(1)).getToken().setText(alias);
      }
    }
  }

  void buildWhereByFB(CommonTree subQCondition, CommonTree compareKeyAlias1,
      CommonTree compareKeyAlias2) {
    this.deleteBranch(bottomSelect,
        PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
    CommonTree where = this.createSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE,
        "where");
    CommonTree logicExpr = this.createSqlASTNode(PantheraParser_PLSQLParser.LOGIC_EXPR,
        "LOGIC_EXPR");
    this.attachChild(where, logicExpr);
    this.attachChild(logicExpr, fb.getASTNode());
    if (compareKeyAlias1 != null && compareKeyAlias2 != null) {
      this.addConditionToWhere(where, FilterBlockUtil.dupNode(subQCondition),
          this
              .createCascatedElement(FilterBlockUtil
                  .cloneTree((CommonTree) compareKeyAlias1.getChild(0))), this
              .createCascatedElement(FilterBlockUtil
                  .cloneTree((CommonTree) compareKeyAlias2.getChild(0))));
    }
    this.attachChild(topSelect, where);
  }

  void builldSimpleWhere(CommonTree condition) {
    CommonTree where = this.createSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE,
        "where");
    CommonTree logicExpr = this.createSqlASTNode(PantheraParser_PLSQLParser.LOGIC_EXPR,
        "LOGIC_EXPR");
    this.attachChild(where, logicExpr);
    this.attachChild(logicExpr, condition);
    this.attachChild(topSelect, where);
  }

  void rebuildGroupOrder(CommonTree topAlias) {
    CommonTree group = topQuery.getGroup();
    CommonTree order = topQuery.getOrder();
    if (group != null) {
      for (int i = 0; i < group.getChildCount(); i++) {
        CommonTree groupElement = (CommonTree) group.getChild(i);
        CommonTree anyElement = (CommonTree) groupElement.getChild(0).getChild(0).getChild(0);
        this.rebuildAnyElementAlias(topAlias, anyElement);
      }
    }
    if (order != null) {
      CommonTree orderByElements = (CommonTree) order.getChild(0);
      for (int i = 0; i < orderByElements.getChildCount(); i++) {
        CommonTree orderByElement = (CommonTree) orderByElements.getChild(i);
        CommonTree anyElement = (CommonTree) orderByElement.getChild(0).getChild(0).getChild(0);
        this.rebuildAnyElementAlias(topAlias, anyElement);
      }
    }
  }

  void rebuildAnyElementAlias(CommonTree topAlias, CommonTree anyElement) {
    CommonTree column;
    if (anyElement.getChildCount() == 2) {
//      ((CommonTree) anyElement.getChild(0)).getToken().setText(topAlias.getChild(0).getText());// TODO
      // remove, needn't table alias.
      anyElement.deleteChild(0);
      column = (CommonTree) anyElement.getChild(0);
    } else {
      column = (CommonTree) anyElement.getChild(0);
    }
    Map<String, String> columnMap = this.columnAliasMap.get(topAlias==null?"":topAlias.getChild(0).getText());
    String columnAlias = columnMap.get(column.getText());
    if (columnAlias != null) {
      column.getToken().setText(columnAlias);
    }
  }

  /**
   * rebuild order&group with alias when create closing select list
   *
   * @param oldAlias
   * @param newAlias
   */
  void reRebuildGroupOrder(CommonTree oldAlias, CommonTree newAlias) {

    CommonTree group = topQuery.getGroup();
    CommonTree order = topQuery.getOrder();
    if (group != null) {
      //should be rebuild group alias in query block, use select_list's column replace alias

//      for (int i = 0; i < group.getChildCount(); i++) {
//        CommonTree groupElement = (CommonTree) group.getChild(i);
//        CommonTree anyElement = (CommonTree) groupElement.getChild(0).getChild(0).getChild(0);
//        this.reRebuildAnyElement(oldAlias, newAlias, anyElement);
//      }
    }
    if (order != null) {
      CommonTree orderByElements = (CommonTree) order.getChild(0);
      for (int i = 0; i < orderByElements.getChildCount(); i++) {
        CommonTree orderByElement = (CommonTree) orderByElements.getChild(i);
        CommonTree anyElement = (CommonTree) orderByElement.getChild(0).getChild(0).getChild(0);
        this.reRebuildAnyElement(oldAlias, newAlias, anyElement);
      }
    }

  }

  private void reRebuildAnyElement(CommonTree oldAlias, CommonTree newAlias, CommonTree anyElement) {

    if (anyElement.getChildCount() == 2) {
      anyElement.deleteChild(0);
    }
    CommonTree child = (CommonTree) anyElement.getChild(0);
    if (child.getText().equals(oldAlias.getChild(0).getText())) {
      child.getToken().setText(newAlias.getChild(0).getText());
    }

  }


  /**
   * for intersect
   *
   * @param selectList
   * @param topAlias
   * @param bottomAlias
   * @return
   */
  CommonTree makeOn(CommonTree topSelectList, CommonTree bottomSelectList, CommonTree topAlias,
      CommonTree bottomAlias) {
    CommonTree on = this.createSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_ON, "on");
    CommonTree logicExpr = this.createSqlASTNode(PantheraParser_PLSQLParser.LOGIC_EXPR,
        "LOGIC_EXPR");
    this.attachChild(on, logicExpr);

    for (int i = 0; i < topSelectList.getChildCount(); i++) {
      CommonTree topAliasId = (CommonTree) topSelectList.getChild(i).getChild(1).getChild(0);
      CommonTree bottomAliasId = (CommonTree) bottomSelectList.getChild(i).getChild(1).getChild(0);
      CommonTree topColumn = FilterBlockUtil.dupNode(topAliasId);
      CommonTree bottomColumn = FilterBlockUtil.dupNode(bottomAliasId);
      CommonTree left = this.createCascatedElement((CommonTree) topAlias.getChild(0), topColumn);
      CommonTree right = this.createCascatedElement((CommonTree) bottomAlias.getChild(0),
          bottomColumn);
      CommonTree equals = this.createSqlASTNode(PantheraParser_PLSQLParser.EQUALS_OP, "=");
      this.attachChild(equals, left);
      this.attachChild(equals, right);
      if (logicExpr.getChildCount() > 0) {
        CommonTree origin = (CommonTree) logicExpr.deleteChild(0);
        CommonTree and = this
            .createSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_AND, "and");
        this.attachChild(and, origin);
        this.attachChild(and, equals);
        this.attachChild(logicExpr, and);
      } else {
        this.attachChild(logicExpr, equals);
      }

    }
    return on;
  }

  /**
   * update selectList's item alias
   *
   * @param selectList
   */
  // void rebuildSelectListAlias(CommonTree selectList){
  // for(int i=0;i<selectList.getChildCount();i++){
  // CommonTree
  // }
  // }

  // void rebuildSelectListAlias(CommonTree selectList,CommonTree tableAlias){
  // for(int i=0;i<selectList.getChildCount();i++){
  // CommonTree selectItem = (CommonTree) selectList.getChild(i);
  // String cloumnAlias = selectItem.getChild(1).getChild(0).getText();
  // CommonTree anyElement =(CommonTree)selectItem.getChild(0).getChild(0).getChild(0);
  // if(anyElement.getChildCount()==2){
  //
  // }
  //
  // }
  // }
}
