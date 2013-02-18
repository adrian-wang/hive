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

import org.antlr33.runtime.tree.CommonTree;
import org.antlr33.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.processor.FilterBlockProcessorFactory;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;


public class QueryBlock extends BaseFilterBlock {

  private Set<String> tableNameSet;
  private CommonTree queryForTransfer;
  private List<CommonTree> aggregationList;
  private CommonTree group;
  private CommonTree order;
  private CommonTree limit;
  private final CountAsterisk countAsterisk = new CountAsterisk();


  /**
   * FIXME just process two level subquery
   */
  @Override
  public void process(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {

    fbContext.getQueryStack().push(this);

    // TODO should process having firstly?(because of group processing...)
    super.processChildren(fbContext, context);

    FilterBlock childFb = this.getChildren().size() == 0 ? null : this.getChildren().get(0);
    // has child filter block & transformed tree
    if (childFb != null && childFb.getTransformedNode() != null) {
      // restore aggregation function
      CommonTree select = childFb.getTransformedNode();
      CommonTree selectList = (CommonTree) select
          .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
      if (selectList != null && aggregationList != null) {
        if (selectList.getChildCount() != aggregationList.size()) {
          throw new SqlXlateException("FATAL ERROR:mismatch select item's size after transformed.");
        }
        for (int i = 0; i < selectList.getChildCount(); i++) {
          CommonTree func = aggregationList.get(i);
          if (func != null) {
            CommonTree selectItem = (CommonTree) selectList.getChild(i);
            CommonTree expr = (CommonTree) selectItem.getChild(0);
            CommonTree cascatedElement = (CommonTree) expr.deleteChild(0);
            expr.addChild(func);
            List<CommonTree> exprList = new ArrayList<CommonTree>();
            FilterBlockUtil.findNode(func, PantheraParser_PLSQLParser.EXPR, exprList);
            CommonTree expr2 = exprList.get(0);
            expr2.addChild(cascatedElement);
          }
        }
      }
      if (countAsterisk.getSelectItem() != null) {
        selectList.getChildren().add(countAsterisk.getPosition(), countAsterisk.getSelectItem());
      }
      this.setTransformedNode(select);
    }


    if (!(this.getParent() instanceof SubQFilterBlock)) {
      // current' is top query block, add transformedNode to origin tree
      if (this.getTransformedNode() != null) {
        CommonTree subQueryNode = (CommonTree) this.getASTNode().getParent();// PantheraParser_PLSQLParser.SUBQUERY
        subQueryNode.deleteChild(0);
        subQueryNode.addChild(this.getTransformedNode());
        if (group != null && fbContext.getTypeStack().isEmpty()) {
          this.getTransformedNode().addChild(group);// restore group;
        }
      }
    } else {// current' is bottom query block
      // FIXME bugs
      if (this.getTransformedNode() == null) {
        if (fbContext.getTypeStack().peek() instanceof HavingFilterBlock) {
          FilterBlockProcessorFactory.getHavingUnCorrelatedProcessor(
              fbContext.getSubQStack().peek()
                  .getASTNode().getType()).process(fbContext, this, context);
        }
        if (fbContext.getTypeStack().peek() instanceof WhereFilterBlock) {
          FilterBlockProcessorFactory.getUnCorrelatedProcessor(fbContext.getSubQStack().peek()
              .getASTNode().getType()).process(fbContext, this, context);
        }
      }
    }

    //limit
    if(limit!=null&&this.getTransformedNode()!=null){
      this.getTransformedNode().addChild(limit);
    }
    fbContext.getQueryStack().pop();


  }

  public void init() {
    recordGroupOrder();
    buildTableNameSet();
    buildQueryForTransfer();
  }

  void recordGroupOrder() {
    group = (CommonTree) this.getASTNode().getFirstChildWithType(
        PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP);
    order = (CommonTree) ((CommonTree) this.getASTNode().getParent().getParent())
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_ORDER);
    limit = (CommonTree) this.getASTNode().getFirstChildWithType(PantheraExpParser.LIMIT_VK);
  }

  void buildTableNameSet() {
    tableNameSet = new HashSet<String>();
  }

  void buildQueryForTransfer() {
    CommonTree root = this.getASTNode();
    CommonTree cloneRoot = FilterBlockUtil.dupNode(root);
    for (int i = 0; i < root.getChildCount(); i++) {
      CommonTree child = (CommonTree) root.getChild(i);
      int type = child.getType();
      if (type == PantheraParser_PLSQLParser.SQL92_RESERVED_FROM
          || type == PantheraParser_PLSQLParser.SELECT_LIST
          || type == PantheraParser_PLSQLParser.ASTERISK) {
        CommonTree clone = FilterBlockUtil.dupNode(child);
        cloneRoot.addChild(clone);
        FilterBlockUtil.cloneTree(clone, child);
      }
    }
    aggregationList = FilterBlockUtil.filterAggregation((CommonTree) cloneRoot
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), countAsterisk);

    queryForTransfer = cloneRoot;
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
    return FilterBlockUtil.cloneTree(queryForTransfer);
  }

  /**
   * clone QueryBlock's query tree
   *
   * @return
   */
  public CommonTree cloneWholeQuery() {
    return FilterBlockUtil.cloneTree(this.getASTNode());
  }

  public void setAggregationList(List<CommonTree> aggregationList) {
    this.aggregationList = aggregationList;
  }

  /**
   * set alias leaf's text to origin leaf
   *
   * @param origin
   * @param alias
   */
  private void convertAlias(CommonTree origin, CommonTree alias) {
    CommonTree o = origin;
    while (o.getChild(0).getChildCount() > 0) {
      o = (CommonTree) o.getChild(0);
    }
    CommonTree a = alias;
    while (a.getChild(0).getChildCount() > 0) {
      a = (CommonTree) a.getChild(0);
    }
    Tree t = o.getParent();
    t.deleteChild(0);
    t.addChild(a);
  }

  public CommonTree getGroup() {
    return group;
  }

  public void setGroup(CommonTree group) {
    this.group = group;
  }

  public CommonTree getOrder() {
    return order;
  }

  /**
   * record count(*) in query
   * CountAsterisk.
   *
   */
  public class CountAsterisk {
    private int position;
    private CommonTree selectItem;

    public int getPosition() {
      return position;
    }

    public void setPosition(int position) {
      this.position = position;
    }

    public CommonTree getSelectItem() {
      return selectItem;
    }

    public void setSelectItem(CommonTree selectItem) {
      this.selectItem = selectItem;
    }

  }

}
