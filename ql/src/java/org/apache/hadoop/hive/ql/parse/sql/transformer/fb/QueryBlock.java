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

import java.util.HashSet;
import java.util.Set;

import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;


public class QueryBlock extends BaseFilterBlock {

  private Set<String> tableNameSet;
  private CommonTree queryForTransfer;



  @Override
  public void process(FilterBlockContext fbContext, TranslateContext context) {

    fbContext.getQueryStack().push(this);

    for (FilterBlock fb : this.getChildren()) {
      fb.process(fbContext, context);
    }

    fbContext.getQueryStack().pop();

  }

  public void init() {
    buildTableNameSet();
    buildQueryForTransfer();
  }

  void buildTableNameSet() {
    tableNameSet = new HashSet<String>();
  }

  void buildQueryForTransfer() {
    CommonTree root = this.getASTNode();
    CommonTree cloneRoot = super.dupNode(root);
    CommonTree from = (CommonTree) root
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
    CommonTree cloneFrom = super.dupNode(from);
    cloneRoot.addChild(cloneFrom);
    CommonTree selectList = (CommonTree) root
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    CommonTree cloneSelectList = super.dupNode(selectList);
    cloneRoot.addChild(cloneSelectList);
    cloneTree(cloneFrom, from);
    cloneFilterTree(cloneSelectList, selectList);
    queryForTransfer = cloneRoot;
  }

  /**
   * clone tree
   *
   * @param clone
   * @param node
   */
  void cloneTree(CommonTree clone, CommonTree node) {
    for (int i = 0; i < node.getChildCount(); i++) {
      CommonTree sub = (CommonTree) node.getChild(i);
      CommonTree cloneSub = super.dupNode(sub);
      clone.addChild(cloneSub);
      cloneTree(cloneSub, sub);
    }
  }

  /**
   * clone tree with filter aggregation function
   *
   * @param clone
   * @param node
   */
  void cloneFilterTree(CommonTree clone, CommonTree node) {
    for (int i = 0; i < node.getChildCount(); i++) {
      CommonTree sub = filter((CommonTree) node.getChild(i));
      CommonTree cloneSub = super.dupNode(sub);
      clone.addChild(cloneSub);
      cloneTree(cloneSub, sub);
    }

  }

  CommonTree filter(CommonTree node) {
    // FIXME just deal with the most simple branch.
    int type = node.getType();
    if (type == PantheraParser_PLSQLParser.STANDARD_FUNCTION) {
      return (CommonTree) node.getChild(0).getChild(0).getChild(0).getChild(0).getChild(0);
      // STANDARD_FUNCTION.functionName.ARGUMENTS.ARGUMENT.EXPR.CASCATED_ELEMENT
    }
    return node;
  }


  public Set<String> getTableNameSet() {
    return tableNameSet;
  }



  /**
   * clone QueryBlock's simple query without where, group...
   *
   * @return
   */
  public CommonTree cloneSimpleQuery() {
    CommonTree root = super.dupNode(queryForTransfer);
    cloneTree(root, queryForTransfer);
    return root;
  }






}
