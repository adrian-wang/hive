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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlASTNode;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Support not equal correlated condition in subquery.
 *
 * NotEqualJoinTransformer.
 *
 */
public class NotEqualJoinTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  public NotEqualJoinTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(SqlASTNode tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    this.transformNotEqualJoin(tree, context);
  }

  private void transformNotEqualJoin(SqlASTNode tree, TranslateContext context)
      throws SqlXlateException {
    Map<CommonTree, List<CommonTree>> joinMap = (Map<CommonTree, List<CommonTree>>) context
        .getBallFromBasket(TranslateContext.JOIN_TYPE_NODE_BALL);
    if (joinMap != null) {
      for (Entry<CommonTree, List<CommonTree>> entry : joinMap.entrySet()) {
        CommonTree joinType = entry.getKey();
        List<CommonTree> notEqualConditionList = entry.getValue();
        if (notEqualConditionList != null && !notEqualConditionList.isEmpty()) {
          if (PantheraExpParser.LEFT_STR.equals(joinType.getText())) {
            processLeftJoin((CommonTree) joinType.getParent(), notEqualConditionList);
          }
          if (PantheraExpParser.LEFTSEMI_STR.equals(joinType.getText())) {

          }
        }
      }
    }
  }

  /**
   * process not equal in left join(from not exists)
   *
   * @param join
   * @param notEqualConditionList
   * @throws SqlXlateException
   */
  private void processLeftJoin(CommonTree join, List<CommonTree> notEqualConditionList)
      throws SqlXlateException {
    String tableAlias = join.getFirstChildWithType(PantheraExpParser.TABLE_REF_ELEMENT).getChild(0)
        .getChild(0).getText();
    CommonTree select = FilterBlockUtil.findOnlyNode(join, PantheraExpParser.SQL92_RESERVED_SELECT);
    CommonTree selectList = (CommonTree) select
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    CommonTree on = FilterBlockUtil.findOnlyNode(join, PantheraExpParser.SQL92_RESERVED_ON);
    CommonTree where = FilterBlockUtil.createSqlASTNode(PantheraExpParser.SQL92_RESERVED_WHERE,
        "where");
    CommonTree logic = FilterBlockUtil.createSqlASTNode(PantheraExpParser.LOGIC_EXPR, "LOGIC_EXPR");
    where.addChild(logic);
    select.addChild(where);
    assert (on != null && selectList != null && tableAlias != null);

    // process join branch
    List<Map<Boolean, List<CommonTree>>> joinKeys = this.getFilterkey(tableAlias, (CommonTree) on
        .getChild(0).getChild(0));
    for (int i = 0; i < joinKeys.size(); i++) {
      List<CommonTree> bottomKeys = joinKeys.get(i).get(false);
      List<CommonTree> topKeys = joinKeys.get(i).get(true);
      CommonTree op = (CommonTree) bottomKeys.get(0).getParent();
      // uncorrelated condition,transfer it to WHERE condition and remove it from SELECT_ITEM
      if (bottomKeys != null && topKeys == null) {
        FilterBlockUtil.deleteTheNode(op);
        logic.addChild(op);
        for (CommonTree bottomkey : bottomKeys) {
          CommonTree anyElement = (CommonTree) bottomkey.getChild(0);
          anyElement.deleteChild(0);
          CommonTree column = (CommonTree) anyElement.getChild(0);
          String columnAlias = column.getText();
          for (int j = 0; j < selectList.getChildCount(); j++) {
            CommonTree selectItem = (CommonTree) selectList.getChild(j);
            String selectAlias = selectItem.getChild(1).getChild(0).getText();
            String selectColumn = selectItem.getChild(0).getChild(0).getChild(0).getChild(0)
                .getText();
            if (selectAlias.equals(columnAlias)) {
              column.getToken().setText(selectColumn);
              selectList.deleteChild(j);
              break;
            }
          }
        }
      }
      // correlated equal condition,group by it, only process one condition now.
      if (bottomKeys != null && topKeys != null && op.getType() == PantheraExpParser.EQUALS_OP) {
        CommonTree group = FilterBlockUtil.createSqlASTNode(PantheraExpParser.SQL92_RESERVED_GROUP,
            "group");
        select.addChild(group);
        CommonTree groupByElement = FilterBlockUtil.createSqlASTNode(
            PantheraExpParser.GROUP_BY_ELEMENT, "GROUP_BY_ELEMENT");
        group.addChild(groupByElement);
        CommonTree expr = FilterBlockUtil.createSqlASTNode(PantheraExpParser.EXPR, "EXPR");
        groupByElement.addChild(expr);
        CommonTree cascatedElement = FilterBlockUtil.cloneTree((CommonTree) bottomKeys.get(0));
        cascatedElement.getChild(0).deleteChild(0);
        String columnAlias = cascatedElement.getChild(0).getChild(0).getText();
        for (int j = 0; j < selectList.getChildCount(); j++) {
          CommonTree selectItem = (CommonTree) selectList.getChild(j);
          String selectAlias = selectItem.getChild(1).getChild(0).getText();
          String selectColumn = selectItem.getChild(0).getChild(0).getChild(0).getChild(0)
              .getText();
          if (selectAlias.equals(columnAlias)) {
            ((CommonTree) cascatedElement.getChild(0).getChild(0)).getToken().setText(selectColumn);
          }
        }
        expr.addChild(cascatedElement);
      }
    }

    // process where branch
    CommonTree whereLogicExpr = (CommonTree) ((CommonTree) join.getParent().getParent().getParent())
        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_WHERE).getChild(0);
    for (CommonTree notEqualNode : notEqualConditionList) {
      Map<Boolean, List<CommonTree>> key = this.getFilter(tableAlias, notEqualNode);
      CommonTree bottomKey = key.get(false).get(0);
      String bottomKeyAilas = bottomKey.getChild(0).getChild(1).getText();
      String countAliasStr = "";
      // process SELECT LIST
      for (int j = 0; j < selectList.getChildCount(); j++) {
        CommonTree selectItem = (CommonTree) selectList.getChild(j);
        String selectAlias = selectItem.getChild(1).getChild(0).getText();
        if (selectAlias.equals(bottomKeyAilas)) {
          // count
          CommonTree countSelectItem = FilterBlockUtil.cloneTree(selectItem);
          CommonTree countAlias = (CommonTree) countSelectItem.getChild(1).getChild(0);
          countAliasStr = countAlias.getText() + "count";
          countAlias.getToken().setText(countAliasStr);
          CommonTree countCascatedElement = (CommonTree) countSelectItem.getChild(0).deleteChild(0);
          CommonTree countStandardFunction = FilterBlockUtil.createSqlASTNode(
              PantheraParser_PLSQLParser.STANDARD_FUNCTION, "STANDARD_FUNCTION");
          CommonTree function = FilterBlockUtil.createSqlASTNode(
              PantheraParser_PLSQLParser.COUNT_VK, "count");
          FilterBlockUtil.attachChild(countStandardFunction, function);
          CommonTree expr = FilterBlockUtil.createSqlASTNode(PantheraParser_PLSQLParser.EXPR,
              "EXPR");
          FilterBlockUtil.attachChild(function, expr);
          FilterBlockUtil.attachChild(expr, countCascatedElement);
          countSelectItem.getChild(0).addChild(countStandardFunction);
          selectList.addChild(countSelectItem);

          // max
          CommonTree maxCascatedElement = (CommonTree) selectItem.getChild(0).deleteChild(0);
          CommonTree standardFunction = FilterBlockUtil.createFunction("max", maxCascatedElement);
          selectItem.getChild(0).addChild(standardFunction);
        }
      }
      // process WHERE
      FilterBlockUtil.deleteTheNode(notEqualNode);
      notEqualNode.getToken().setType(PantheraExpParser.EQUALS_OP);
      notEqualNode.getToken().setText("=");

      CommonTree equal = FilterBlockUtil.createSqlASTNode(PantheraExpParser.EQUALS_OP, "=");
      CommonTree cascated = FilterBlockUtil.createSqlASTNode(PantheraExpParser.CASCATED_ELEMENT,
          "CASCATED_ELEMENT");
      equal.addChild(cascated);
      CommonTree anyElement = FilterBlockUtil.createSqlASTNode(PantheraExpParser.ANY_ELEMENT,
          "ANY_ELEMENT");
      cascated.addChild(anyElement);
      CommonTree id = FilterBlockUtil.createSqlASTNode(PantheraExpParser.ID, countAliasStr);
      anyElement.addChild(id);
      CommonTree one = FilterBlockUtil.createSqlASTNode(PantheraExpParser.UNSIGNED_INTEGER, "1");
      equal.addChild(one);
      CommonTree and = FilterBlockUtil
          .createSqlASTNode(PantheraExpParser.SQL92_RESERVED_AND, "and");
      and.addChild(equal);
      and.addChild(notEqualNode);
      CommonTree or = FilterBlockUtil.createSqlASTNode(PantheraExpParser.SQL92_RESERVED_OR, "or");
      or.addChild(and);
      CommonTree oldCondition = (CommonTree) whereLogicExpr.deleteChild(0);
      or.addChild(oldCondition);
      whereLogicExpr.addChild(or);
    }
  }

  /**
   * extract join key from filter op node.
   *
   * @return false: bottom keys<br>
   *         true: top key
   * @throws SqlXlateException
   */
  Map<Boolean, List<CommonTree>> getFilter(String currentTableAlias, CommonTree filterOp)
      throws SqlXlateException {
    Map<Boolean, List<CommonTree>> result = new HashMap<Boolean, List<CommonTree>>();
    for (int i = 0; i < filterOp.getChildCount(); i++) {
      CommonTree child = (CommonTree) filterOp.getChild(i);
      if (child.getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT) {
        String tableAlias = child.getChild(0).getChild(0).getText();
        if (currentTableAlias.equals(tableAlias)) {// uncorrelated
          List<CommonTree> uncorrelatedList = result.get(false);
          if (uncorrelatedList == null) {
            uncorrelatedList = new ArrayList<CommonTree>();
            result.put(false, uncorrelatedList);
          }
          uncorrelatedList.add(child);
        }
        if (!currentTableAlias.equals(tableAlias)) {// correlated
          List<CommonTree> correlatedList = result.get(true);
          if (correlatedList == null) {
            correlatedList = new ArrayList<CommonTree>();
            result.put(true, correlatedList);
          }
          correlatedList.add(child);
        }
      }
    }
    return result;

  }

  /**
   *
   * @return
   * @throws SqlXlateException
   */
  List<Map<Boolean, List<CommonTree>>> getFilterkey(String currentTableAlias, CommonTree condition)
      throws SqlXlateException {
    List<Map<Boolean, List<CommonTree>>> result = new ArrayList<Map<Boolean, List<CommonTree>>>();
    this.getWhereKey(currentTableAlias, condition, result);
    return result;
  }

  private void getWhereKey(String currentTableAlias, CommonTree filterOp,
      List<Map<Boolean, List<CommonTree>>> list) throws SqlXlateException {
    if (FilterBlockUtil.isFilterOp(filterOp)) {
      list.add(getFilter(currentTableAlias, filterOp));
      return;
    } else if (FilterBlockUtil.isLogicOp(filterOp)) {
      for (int i = 0; i < filterOp.getChildCount(); i++) {
        getWhereKey(currentTableAlias, (CommonTree) filterOp.getChild(i), list);
      }
    } else {
      throw new SqlXlateException("unknow filter operation:" + filterOp.getText());
    }

  }
}
