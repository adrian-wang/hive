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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.antlr33.runtime.CommonToken;
import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class FilterBlockUtil {
  private FilterBlockUtil() {
  }

  public static CommonTree dupNode(CommonTree node) {
    return new CommonTree(new CommonToken(node.getToken().getType(), node.getToken().getText()));
  }


  /**
   * clone all branch of tree
   *
   * @param clone
   * @param node
   */
  public static void cloneTree(CommonTree clone, CommonTree node) {
    for (int i = 0; i < node.getChildCount(); i++) {
      CommonTree sub = (CommonTree) node.getChild(i);
      CommonTree cloneSub = FilterBlockUtil.dupNode(sub);
      clone.addChild(cloneSub);
      cloneTree(cloneSub, sub);
    }
  }

  /**
   * filter aggregation function from SELECT_LIST
   *
   * @param selectList
   * @return filtered aggregation function list.It's size equals SELECT_ITEM's number.
   */
  static List<CommonTree> filterAggregation(CommonTree selectList) {
    if (selectList == null) {
      return null;
    }
    List<CommonTree> aggregationList = new ArrayList<CommonTree>();
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      CommonTree expr = (CommonTree) selectItem.getChild(0);
      List<CommonTree> nodeList = new ArrayList<CommonTree>();
      findNode(expr, PantheraParser_PLSQLParser.STANDARD_FUNCTION, nodeList);
      if (nodeList.size() > 0) {// only one supported now, easy to more than one.
        CommonTree standardFunction = nodeList.get(0);
        // FIXME count,count(*),sum, they have different branch shape
        // STANDARD_FUNCTION.functionName.ARGUMENTS.ARGUMENT.EXPR.CASCATED_ELEMENT
        CommonTree expr2 = (CommonTree) standardFunction.getChild(0).getChild(0).getChild(0)
            .getChild(0);
        CommonTree cascatedElement = (CommonTree) expr2.deleteChild(0);
        CommonTree parent = (CommonTree) standardFunction.getParent();
        for (int j = 0; j < parent.getChildCount(); j++) {
          if (parent.getChild(0) == standardFunction) {
            parent.deleteChild(i);
            if (parent.getChildren() == null) {
              parent.addChild(cascatedElement);
            } else {
              parent.getChildren().add(i, cascatedElement);
            }
          }
        }

        aggregationList.add(cloneTree(standardFunction));



        expr.deleteChild(0);
        expr.addChild(cascatedElement);
      } else {
        aggregationList.add(null);
      }
    }
    return aggregationList;
  }

  /**
   * find all node which type is input type in the tree which root is node.
   *
   * @param node
   * @param type
   * @param nodeList
   */
  static void findNode(CommonTree node, int type, List<CommonTree> nodeList) {
    if (node == null) {
      return;
    }
    if (node.getType() == type) {
      nodeList.add(node);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      findNode((CommonTree) node.getChild(i), type, nodeList);
    }
  }

  /**
   * clone tree
   *
   * @param tree
   *          origin tree
   * @return clone tree root
   */
  public static CommonTree cloneTree(CommonTree tree) {
    if (tree == null) {
      return null;
    }
    CommonTree root = dupNode(tree);
    cloneTree(root, tree);
    return root;
  }

  /**
   * get tableName or alias from select node.
   *
   * @param select
   * @return
   */
  public static Set<String> getTableName(CommonTree select) {
    Set<String> result = new HashSet<String>();
    SqlXlateUtil.getSrcTblAndAlias((CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM), result);
    return result;
  }

  public static CommonTree deleteBranch(CommonTree root, int branchType) {
    if (root == null) {
      return null;
    }
    for (int i = 0; i < root.getChildCount(); i++) {
      if (root.getChild(i).getType() == branchType) {
        return (CommonTree) root.deleteChild(i);

      }
    }
    return null;
  }
}
