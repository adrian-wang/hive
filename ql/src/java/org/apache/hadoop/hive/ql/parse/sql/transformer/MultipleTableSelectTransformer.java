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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr33.runtime.tree.CommonTree;
import org.antlr33.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.parse.sql.SqlASTNode;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.QueryInfo.Column;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Transformer for multiple-table select.
 *
 */
public class MultipleTableSelectTransformer extends BaseSqlASTTransformer {
  SqlASTTransformer tf;

  private static class JoinPair<T> {
    private final T first;
    private final T second;

    public JoinPair(T first, T second) {
      this.first = first;
      this.second = second;
    }

    public T getFirst() {
      return first;
    }

    public T getSecond() {
      return second;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof JoinPair<?>)) {
        return false;
      }
      JoinPair<T> otherPair = (JoinPair<T>) other;
      return (first.equals(otherPair.first) && second.equals(otherPair.second)) ||
        (first.equals(otherPair.second) && second.equals(otherPair.first));
    }

    @Override
    public int hashCode() {
      return first.hashCode() ^ second.hashCode();
    }
  }

  private class JoinInfo {
    public Map<JoinPair<String>, List<SqlASTNode>> joinPairInfo = new HashMap<JoinPair<String>, List<SqlASTNode>>();
    public Map<String, List<SqlASTNode>> joinFilterInfo = new HashMap<String, List<SqlASTNode>>();
  }

  public MultipleTableSelectTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(SqlASTNode tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    for (QueryInfo qf : context.getqInfoList()) {
      transformQuery(qf, qf.getSelectKeyForThisQ());
      // Update the from in the query info in case it was changed by the transformer.
      qf.setFrom((SqlASTNode) qf.getSelectKeyForThisQ().getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM));
    }
  }

 private void transformQuery(QueryInfo qf, CommonTree node) throws SqlXlateException {
    if(node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT){
      //
      // Check if this is a multiple table select.
      //
      SqlASTNode from = (SqlASTNode) node.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
      if (from.getChildCount() > 1) {
        JoinInfo joinInfo = new JoinInfo();
        Set<String> srcTables = qf.getSrcTblAliasForSelectKey(node);

        //
        // Transform the where condition and generate the join operation info.
        //
        SqlASTNode where = (SqlASTNode) node.getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
        if (where != null) {
          transformWhereCondition((SqlASTNode) where.getChild(0).getChild(0), srcTables, joinInfo);
        }
        //
        // Transform the from clause tree using the generated join operation info.
        //
        transformFromClause(from, srcTables, joinInfo);
      }
    }

    //
    // Transform subqueries in this query.
    //
    for (int i = 0; i < node.getChildCount(); i++) {
      SqlASTNode child = (SqlASTNode) node.getChild(i);
      if (child.getType() != PantheraParser_PLSQLParser.SQL92_RESERVED_FROM) {
        transformQuery(qf, child);
      }
    }
  }

  private void transformWhereCondition(SqlASTNode node, Set<String> srcTables, JoinInfo joinInfo) throws SqlXlateException {
    //
    // We can only transform equality expression between two columns whose ancesotors are all AND operators
    // into JOIN on ...
    //
    if (node.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_AND) {
      transformWhereCondition((SqlASTNode) node.getChild(0), srcTables, joinInfo);  // Transform the left child.
      transformWhereCondition((SqlASTNode) node.getChild(1), srcTables, joinInfo);  // Transform the right child.

      SqlASTNode leftChild = (SqlASTNode) node.getChild(0);
      SqlASTNode rightChild = (SqlASTNode) node.getChild(1);

      if (leftChild.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_TRUE) {
        //
        // Replace the current node with the right child.
        //
        node.getParent().setChild(node.getChildIndex(), rightChild);
      } else if (rightChild.getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_TRUE) {
        //
        // Replace the current node with the left child.
        //
        node.getParent().setChild(node.getChildIndex(), leftChild);
      }
    } else {
      if (node.getType() == PantheraParser_PLSQLParser.EQUALS_OP) {
        //
        // Check if this is a equality expression between two columns
        //
        if (IsColumnRef(node.getChild(0)) && IsColumnRef(node.getChild(1))) {
          Column column1 = getColumn((SqlASTNode) node.getChild(0).getChild(0), srcTables);
          Column column2 = getColumn((SqlASTNode) node.getChild(1).getChild(0), srcTables);

          //
          // Skip columns not in a src table.
          //
          if (column1.getTblAlias() == null || column2.getTblAlias() == null) {
            return;
          }
          //
          // Update join info.
          //
          JoinPair<String> tableJoinPair = new JoinPair<String>(column1.getTblAlias(), column2.getTblAlias());
          List<SqlASTNode> joinEqualityNodes = joinInfo.joinPairInfo.get(tableJoinPair);
          if (joinEqualityNodes == null) {
            joinEqualityNodes = new ArrayList<SqlASTNode>();
          }
          joinEqualityNodes.add(node);
          joinInfo.joinPairInfo.put(tableJoinPair, joinEqualityNodes);

          //
          // Create a new TRUE node and replace the current node with this new node.
          //
          SqlASTNode trueNode = SqlXlateUtil.newSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_TRUE, "true");
          node.getParent().setChild(node.getChildIndex(), trueNode);
          return;
        }
      }

      //
      // For a where condition that refers a single column and no subquery, then it can be a join filter.
      //
      int columnRefCount = 0;
      int childIndex = 0;
      for (int i = 0; i < node.getChildCount(); i++) {
        if (IsColumnRef(node.getChild(i))) {
          childIndex = i;
          ++columnRefCount;
        }
      }
      if (columnRefCount == 1 && !SqlXlateUtil.hasNodeTypeInTree(node, PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT)) {
        Column column = getColumn((SqlASTNode) node.getChild(childIndex).getChild(0), srcTables);
        String srcTable = column.getTblAlias();
        //
        // Skip if the column is not in any of the src tables.
        //
        if (srcTable == null) {
          return;
        }
        //
        // Update join info.
        //
        List<SqlASTNode> joinFilterNodes = joinInfo.joinFilterInfo.get(srcTable);
        if (joinFilterNodes == null) {
          joinFilterNodes = new ArrayList<SqlASTNode>();
        }
        joinFilterNodes.add(node);
        joinInfo.joinFilterInfo.put(srcTable, joinFilterNodes);
        //
        // Create a new TRUE node and replace the current node with this new node.
        //
        SqlASTNode trueNode = SqlXlateUtil.newSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_TRUE, "true");
        node.getParent().setChild(node.getChildIndex(), trueNode);
      }
    }
  }

  private boolean IsColumnRef (Tree node) {
    if (node.getType() == PantheraParser_PLSQLParser.CASCATED_ELEMENT &&
        node.getChild(0).getType() == PantheraParser_PLSQLParser.ANY_ELEMENT) {
      return true;
    } else {
      return false;
    }
  }

  private Column getColumn(SqlASTNode anyElement, Set<String> srcTables) throws SqlXlateException {
    String columnName;
    String table;

    Column column = new Column();
    if (anyElement.getChildCount() > 1) {
      table = anyElement.getChild(0).getText();
      //
      // Return null table name if it is not a src table.
      //
      if (srcTables.contains(table)) {
        column.setTblAlias(table);
      }
      column.setColAlias(anyElement.getChild(1).getText());
    } else {
      columnName = anyElement.getChild(0).getText();
      column.setTblAlias(getTableForColInThisQ(columnName));
      column.setColAlias(columnName);
    }

    return column;
  }

  private String getTableForColInThisQ(String columeName) throws SqlXlateException {
    // FIXME!
    throw new SqlXlateException("Multi-table selection transformer: table name must be explicitly specified for a column!");
  }

  private void transformFromClause(SqlASTNode oldFrom, Set<String> srcTables, JoinInfo joinInfo) throws SqlXlateException {
    //
    // Check if there is any join operation in the from clause. If yes, such case is not supported and TBD.
    //
    for (int i = 0; i < oldFrom.getChildCount(); i++) {
      if (oldFrom.getChild(i).getChildCount() != 1) {
        throw new SqlXlateException("Multi-table selection transformer: join operation not supported in the from clause!");
      }
    }

    //
    // Create new From node and its child Table Ref node.
    // Replace the old Form node with the new From node.
    //
    SqlASTNode newFrom = SqlXlateUtil.newSqlASTNode(oldFrom);
    SqlASTNode newTableRef = SqlXlateUtil.newSqlASTNode((SqlASTNode) oldFrom.getChild(0));
    newFrom.addChild(newTableRef);
    oldFrom.getParent().setChild(oldFrom.getChildIndex(), newFrom);

    //
    // Iterate the join info to generate a new from sub-tree.
    //

    Set<String> alreadyJoinedTables = new HashSet<String>();
    if (!joinInfo.joinPairInfo.isEmpty()) {
      Set<JoinPair<String>> tableJoinPairs = joinInfo.joinPairInfo.keySet();

      Iterator<JoinPair<String>> iterator = tableJoinPairs.iterator();
      JoinPair<String> tableJoinPair = iterator.next();
      generateTableRefElement(tableJoinPair.getFirst(), oldFrom, newTableRef);
      generateJoin(tableJoinPair.getSecond(), joinInfo.joinPairInfo.get(tableJoinPair), joinInfo.joinFilterInfo.get(tableJoinPair.getSecond()), oldFrom, newTableRef);
      addJoinCondition(joinInfo.joinFilterInfo.get(tableJoinPair.getFirst()), (SqlASTNode) newTableRef.getChild(newTableRef.getChildCount() - 1).getChild(1).getChild(0));
      alreadyJoinedTables.add(tableJoinPair.getFirst());
      alreadyJoinedTables.add(tableJoinPair.getSecond());
      iterator.remove();

      boolean newJoinItem;
      do {
        newJoinItem = false;
        for (iterator = tableJoinPairs.iterator(); iterator.hasNext();) {
          tableJoinPair = iterator.next();
          if (!alreadyJoinedTables.contains(tableJoinPair.getFirst()) && !alreadyJoinedTables.contains(tableJoinPair.getSecond())) {
            continue;
          } else if (alreadyJoinedTables.contains(tableJoinPair.getFirst()) && alreadyJoinedTables.contains(tableJoinPair.getSecond())) {
            int firstIndex;
            for (firstIndex = 0; firstIndex < newTableRef.getChildCount() - 1; firstIndex++) {
              if (SqlXlateUtil.containTableName(tableJoinPair.getFirst(), newTableRef.getChild(firstIndex))) {
                break;
              }
            }
            int secondIndex;
            for (secondIndex = 0; secondIndex < newTableRef.getChildCount() - 1; secondIndex++) {
              if (SqlXlateUtil.containTableName(tableJoinPair.getSecond(), newTableRef.getChild(secondIndex))) {
                break;
              }
            }
            int childIndex = firstIndex > secondIndex ? firstIndex : secondIndex;
            SqlASTNode expressionRoot = (SqlASTNode) newTableRef.getChild(childIndex).getChild(1).getChild(0);
            addJoinCondition(joinInfo.joinPairInfo.get(tableJoinPair), expressionRoot);
          } else if (alreadyJoinedTables.contains(tableJoinPair.getFirst())) {
            generateJoin(tableJoinPair.getSecond(), joinInfo.joinPairInfo.get(tableJoinPair), joinInfo.joinFilterInfo.get(tableJoinPair.getSecond()), oldFrom, newTableRef);
            alreadyJoinedTables.add(tableJoinPair.getSecond());
          } else {
            generateJoin(tableJoinPair.getFirst(), joinInfo.joinPairInfo.get(tableJoinPair), joinInfo.joinFilterInfo.get(tableJoinPair.getFirst()), oldFrom, newTableRef);
            alreadyJoinedTables.add(tableJoinPair.getFirst());
          }
          iterator.remove();
          newJoinItem = true;
        }
      } while (newJoinItem);

      if (!tableJoinPairs.isEmpty()) {
        //
        // Complex cases invovled generation of new subquery is not supported and TBD.
        //
        throw new SqlXlateException("Multi-table selection transformer: Complex cases invovled generation of new subquery is not supported!");
      }
    }

    //
    // Generate cross joins for the left source tables.
    //
    String preTable = null;
    for (String srcTable : srcTables) {
      if (!alreadyJoinedTables.contains(srcTable)) {
        if (newTableRef.getChildCount() == 0) {
          generateTableRefElement(srcTable, oldFrom, newTableRef);
          preTable = srcTable;
        } else {
          List<SqlASTNode> joinFilters = joinInfo.joinFilterInfo.get(srcTable);
          if (preTable != null) {
            List<SqlASTNode> preJoinFilters = joinInfo.joinFilterInfo.get(preTable);
            preTable = null;

            if (preJoinFilters != null) {
              //
              // Merge the first table's join filters of the join sequence into next table.
              //
              if (joinFilters == null) {
                joinFilters = preJoinFilters;
              } else {
                for (SqlASTNode preJoinFilter : preJoinFilters) {
                  joinFilters.add(preJoinFilter);
                }
              }
            }
          }
          generateCrossJoin(srcTable, joinFilters, oldFrom, newTableRef);
        }
      }
    }
  }

  private void generateTableRefElement(String tableName, SqlASTNode oldFrom, SqlASTNode parent) {
    //
    // Find which child of the old From tree contains the table name.
    //
    for (int i = 0; i < oldFrom.getChildCount(); i++) {
      Tree tableRefTree = oldFrom.getChild(i);
      if (SqlXlateUtil.containTableName(tableName, tableRefTree)) {
        oldFrom.deleteChild(i);
        //
        // Move the table ref element tree from oldFrom as the first child of the new table ref node.
        //
        parent.addChild(tableRefTree.getChild(0));
        break;
      }
    }
  }

  private void generateJoin(String tableName, List<SqlASTNode> joinEqualityNodes, List<SqlASTNode> joinFilterNodes, SqlASTNode oldFrom, SqlASTNode tableRef) {
    //
    // Create a Join node and attach it to the new table as the last child.
    //
    SqlASTNode joinNode = SqlXlateUtil.newSqlASTNode(PantheraParser_PLSQLParser.JOIN_DEF, "join");
    tableRef.addChild(joinNode);
    //
    // Generate the table ref element tree as the first child of the join node.
    //
    generateTableRefElement(tableName, oldFrom, joinNode);
    //
    // Generate the join condition sub-tree.
    //
    SqlASTNode onNode = SqlXlateUtil.newSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_ON, "on");
    joinNode.addChild(onNode);

    SqlASTNode logicExprNode = SqlXlateUtil.newSqlASTNode(PantheraParser_PLSQLParser.LOGIC_EXPR, "LOGIC_EXPR");
    onNode.addChild(logicExprNode);

    addJoinCondition(joinEqualityNodes, logicExprNode);
    addJoinCondition(joinFilterNodes, logicExprNode);
  }

  private void addJoinCondition(List<SqlASTNode> joinConditionNodes, SqlASTNode logicExpr) {
    if (joinConditionNodes == null) {
      return;
    }

    Iterator<SqlASTNode> iterator = joinConditionNodes.iterator();
    SqlASTNode expressionRoot;
    if (logicExpr.getChildCount() == 0) {
      expressionRoot =  iterator.next();
    } else {
      expressionRoot = (SqlASTNode) logicExpr.getChild(0);
    }

    while(iterator.hasNext()) {
      SqlASTNode andNode = SqlXlateUtil.newSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_AND, "and");
      andNode.addChild(expressionRoot);
      andNode.addChild(iterator.next());
      expressionRoot = andNode;
    }

    if (logicExpr.getChildCount() == 0) {
      logicExpr.addChild(expressionRoot);
    } else {
      logicExpr.setChild(0, expressionRoot);
    }
  }

  private void generateCrossJoin(String tableName, List<SqlASTNode> joinFilterNodes, SqlASTNode oldFrom, SqlASTNode tableRef) {
    //
    // Create a Join node and attach it to the new table as the last child.
    //
    SqlASTNode joinNode = SqlXlateUtil.newSqlASTNode(PantheraParser_PLSQLParser.JOIN_DEF, "join");
    tableRef.addChild(joinNode);
    //
    // Create a Cross node and attach it to the join node as the first child.
    //
    SqlASTNode crossNode = SqlXlateUtil.newSqlASTNode(PantheraParser_PLSQLParser.CROSS_VK, "cross");
    joinNode.addChild(crossNode);
    //
    // Generate the table ref element tree as the second child of the join node.
    //
    generateTableRefElement(tableName, oldFrom, joinNode);

    if (joinFilterNodes != null) {
      //
      // Generate the join condition sub-tree.
      //
      SqlASTNode onNode = SqlXlateUtil.newSqlASTNode(PantheraParser_PLSQLParser.SQL92_RESERVED_ON, "on");
      joinNode.addChild(onNode);

      SqlASTNode logicExprNode = SqlXlateUtil.newSqlASTNode(PantheraParser_PLSQLParser.LOGIC_EXPR, "LOGIC_EXPR");
      onNode.addChild(logicExprNode);

      addJoinCondition(joinFilterNodes, logicExprNode);
    }
  }
}
