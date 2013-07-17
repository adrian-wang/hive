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
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlASTNode;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

/**
 * transform UNION, INTERSECT, MINUS SetOperatorTransformer.
 *
 */
public class SetOperatorTransformer extends BaseSqlASTTransformer {

  SqlASTTransformer tf;

  public SetOperatorTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(SqlASTNode tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    trans(tree, context);
  }

  private void trans(CommonTree node, TranslateContext context) {
    int childCount = node.getChildCount();
    for (int i = 0; i < childCount; i++) {
      int index = i;
      if (node.getType() == PantheraExpParser.SUBQUERY) {
        // for multiple UNION
        Integer reduceChildCount = (Integer) context.getBallFromBasket(node);
        if (reduceChildCount != null) {
          index = index - reduceChildCount;
        }
      }
      CommonTree child = (CommonTree) node.getChild(index);
      trans(child, context);
    }
    if (node.getType() == PantheraExpParser.SQL92_RESERVED_UNION) {
      processUnion((CommonTree) node.getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_ALL),
          node, context);
    }
  }

  private void processUnion(CommonTree all, CommonTree node, TranslateContext context) {
    int nodeIndex = node.getChildIndex();
    assert (nodeIndex > 0);
    CommonTree parent = (CommonTree) node.getParent();
    CommonTree leftSelect = (CommonTree) parent.getChild(nodeIndex - 1);
    CommonTree rightSelect = (CommonTree) node
        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_SELECT);
    if (rightSelect == null) {
      rightSelect = (CommonTree) ((CommonTree) node
          .getFirstChildWithType(PantheraExpParser.SUBQUERY))
          .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_SELECT);
    }
    if (leftSelect == null || rightSelect == null) {
      return;
    }

    // for different column name in both tables of UNION(Hive don't support it)
    FilterBlockUtil.addColumnAlias(leftSelect, context);
    FilterBlockUtil.addColumnAlias(rightSelect, context);

    CommonTree select = FilterBlockUtil.createSqlASTNode(PantheraExpParser.SQL92_RESERVED_SELECT,
        "select");
    CommonTree subquery = FilterBlockUtil.makeSelectBranch(select, context);
    subquery.addChild(leftSelect);
    subquery.addChild(node);
    CommonTree selectList = FilterBlockUtil.cloneSelectListByAlias((CommonTree) leftSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST));
    if (all == null) {
      select.addChild(FilterBlockUtil.createSqlASTNode(PantheraExpParser.SQL92_RESERVED_DISTINCT,
          "distinct"));
    }
    select.addChild(selectList);
    parent.deleteChild(nodeIndex - 1);
    SqlXlateUtil.addCommonTreeChild(parent, nodeIndex - 1, select);
    parent.deleteChild(nodeIndex);
    Integer reduceChildCount = (Integer) context.getBallFromBasket(parent);
    if (reduceChildCount == null) {
      context.putBallToBasket(parent, 1);
    } else {
      context.putBallToBasket(parent, reduceChildCount + 1);
    }
  }
}
