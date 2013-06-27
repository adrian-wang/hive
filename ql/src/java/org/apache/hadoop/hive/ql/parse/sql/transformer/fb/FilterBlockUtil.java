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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.QueryBlock.CountAsterisk;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class FilterBlockUtil {
  private static final Log LOG = LogFactory.getLog(FilterBlockUtil.class);

  private FilterBlockUtil() {
  }

  public static CommonTree dupNode(CommonTree node) {
    if (node == null) {
      return null;
    }
    CommonTree result = null;
    try {
      result = new CommonTree(new CommonToken(node.getToken().getType(), node.getToken().getText()));
    } catch (Exception e) {
      LOG.error("ERROR Node:" + node);
    }
    return result;
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
  static List<CommonTree> filterAggregation(CommonTree selectList, CountAsterisk countAsterisk) {
    if (selectList == null) {
      return null;
    }
    List<CommonTree> aggregationList = new ArrayList<CommonTree>();
    for (int i = 0; i < selectList.getChildCount(); i++) {
      CommonTree selectItem = (CommonTree) selectList.getChild(i);
      CommonTree expr = (CommonTree) selectItem.getChild(0);
      List<CommonTree> standardFunctionList = new ArrayList<CommonTree>();
      findNode(expr, PantheraParser_PLSQLParser.STANDARD_FUNCTION, standardFunctionList);
      // TODO only one function supported now, hard to more than one.
      // FIXME support complex expression without function(such as (col*3)/2)
      // TODO these code(and QueryBlock's) need clear
      if (standardFunctionList.size() == 1) {

        CommonTree standardFunction = standardFunctionList.get(0);
        // TODO only support a,count(*) style ,NOT support select count(*) from...
        if (standardFunction.getChild(0).getType() == PantheraParser_PLSQLParser.COUNT_VK
            && standardFunction.getChild(0).getChild(0).getType() == PantheraParser_PLSQLParser.ASTERISK) {
          countAsterisk.setPosition(i);
          countAsterisk.setSelectItem(selectItem);
          continue;
        }
        List<CommonTree> exprList = new ArrayList<CommonTree>();
        findNode(standardFunction, PantheraParser_PLSQLParser.EXPR, exprList);
        CommonTree expr2 = exprList.get(0);
        CommonTree cascatedElement = (CommonTree) expr2.deleteChild(0);
        CommonTree func = cloneTree((CommonTree) expr.getChild(0));
        CommonTree parent = (CommonTree) standardFunction.getParent();
        for (int j = 0; j < parent.getChildCount(); j++) {
          if (parent.getChild(j) == standardFunction) {
            parent.deleteChild(j);
            if (parent.getChildren() == null) {
              parent.addChild(cascatedElement);
            } else {
              parent.getChildren().add(j, cascatedElement);
            }
          }
        }
        aggregationList.add(func);
        expr.deleteChild(0);
        expr.addChild(cascatedElement);
      } else {
        aggregationList.add(null);
      }
    }
    if (countAsterisk.getSelectItem() != null) {
      selectList.deleteChild(countAsterisk.getPosition());
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
  public static void findNode(CommonTree node, int type, List<CommonTree> nodeList) {
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

  public static CommonTree findOnlyNode(CommonTree node, int type) {
    List<CommonTree> nodeList = new ArrayList<CommonTree>();
    findNode(node, type, nodeList);
    return nodeList.isEmpty() ? null : nodeList.get(0);
  }

  public static void findNodeText(CommonTree node, String text, List<CommonTree> nodeList) {
    if (node == null) {
      return;
    }
    if (node.getText().equals(text)) {
      nodeList.add(node);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      findNodeText((CommonTree) node.getChild(i), text, nodeList);
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

  public static CommonTree createSqlASTNode(int type, String text) {
    return new CommonTree(new CommonToken(type, text));
  }

  public static void attachChild(CommonTree parent, CommonTree child) {
    if (parent != null) {
      parent.addChild(child);
      if (child != null) {
        child.setParent(parent);
      }
    }
  }

  public static CommonTree createAlias(TranslateContext context) {
    CommonTree alias = createSqlASTNode(PantheraParser_PLSQLParser.ALIAS, "ALIAS");
    CommonTree aliasName = createSqlASTNode(PantheraParser_PLSQLParser.ID, context.getAliasGen()
        .generateAliasName());
    attachChild(alias, aliasName);
    return alias;
  }

  public static CommonTree createFunction(String functionName, CommonTree element) {
    CommonTree standardFunction = createSqlASTNode(PantheraParser_PLSQLParser.STANDARD_FUNCTION,
        "STANDARD_FUNCTION");
    CommonTree function = createSqlASTNode(PantheraParser_PLSQLParser.FUNCTION_ENABLING_OVER,
        functionName);
    attachChild(standardFunction, function);
    CommonTree arguments = createSqlASTNode(PantheraParser_PLSQLParser.ARGUMENTS, "ARGUMENTS");
    attachChild(function, arguments);
    CommonTree argument = createSqlASTNode(PantheraParser_PLSQLParser.ARGUMENT, "ARGUMENT");
    attachChild(arguments, argument);
    CommonTree expr = createSqlASTNode(PantheraParser_PLSQLParser.EXPR, "EXPR");
    attachChild(argument, expr);
    attachChild(expr, element);
    return standardFunction;
  }

  public static boolean isFilterOp(CommonTree node) {
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

  public static boolean isLogicOp(CommonTree node) {
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
   * delete the node and re-balance the tree
   * @param op
   */
  public static void deleteTheNode(CommonTree op){
    int opIndex = op.getChildIndex();
    CommonTree parent = (CommonTree) op.getParent();
    int parentIndex = parent.getChildIndex();
    CommonTree grandpa = (CommonTree) parent.getParent();
    CommonTree brother = (CommonTree) parent.getChild(opIndex == 0 ? 1 : 0);
    grandpa.deleteChild(parentIndex);
    SqlXlateUtil.addCommonTreeChild(grandpa, parentIndex, brother);
  }
}
