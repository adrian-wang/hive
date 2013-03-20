package org.apache.hadoop.hive.ql.parse.sql.transformer.fb.processor;

import java.util.ArrayList;
import java.util.List;

import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.CorrelatedFilterBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.SubQFilterBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.UnCorrelatedFilterBlock;

public class AndProcessor extends CommonFilterBlockProcessor {

  /**
   * template method
   */
  @Override
  public void process(FilterBlockContext fbContext, FilterBlock fb, TranslateContext context)
      throws SqlXlateException {
    // processAndByIntersect(fbContext, fb, context);
    processAndByJoin(fbContext, fb, context);
  }

  /**
   * process and node by intersect.
   * may be use it to process the situation: correlated AND subquery.
   *
   * @param fbContext
   * @param fb
   * @param context
   * @throws SqlXlateException
   * @deprecated
   */
  @Deprecated
  private void processAndByIntersect(FilterBlockContext fbContext, FilterBlock fb,
      TranslateContext context)
      throws SqlXlateException {
    topQuery = fbContext.getQueryStack().firstElement();// TODO how about group?
    topSelect = fb.getChildren().get(0).getTransformedNode();
    bottomSelect = fb.getChildren().get(1).getTransformedNode();
    this.fbContext = fbContext;
    this.fb = fb;
    this.context = context;
    processFB();
    fb.setTransformedNode(super.topSelect);

  }

  @Override
  void processFB() throws SqlXlateException {
    super.processAnd(FilterBlockUtil.createSqlASTNode(PantheraExpParser.LEFTSEMI_VK, "leftsemi"));
  }

  /**
   * process And node by transform AND logic to join
   *
   * @param fbContext
   * @param fb
   * @param context
   */
  private void processAndByJoin(FilterBlockContext fbContext, FilterBlock fb,
      TranslateContext context)
      throws SqlXlateException {
    FilterBlock leftFB = fb.getChildren().get(0);
    FilterBlock rightFB = fb.getChildren().get(1);
    FilterBlock simpleFilter;
    FilterBlock subqFilter;
    CommonTree transformeredNode;
    // only one filterBlock is UnCorrelatedFilterBlock
    if (leftFB instanceof UnCorrelatedFilterBlock && rightFB instanceof SubQFilterBlock) {
      simpleFilter = leftFB;
      subqFilter = rightFB;
      transformeredNode = processUnCorrelated(simpleFilter, subqFilter);
    } else if (rightFB instanceof UnCorrelatedFilterBlock && leftFB instanceof SubQFilterBlock) {
      simpleFilter = rightFB;
      subqFilter = leftFB;
      transformeredNode = processUnCorrelated(simpleFilter, subqFilter);
    } else if (!(leftFB instanceof CorrelatedFilterBlock || rightFB instanceof CorrelatedFilterBlock)) {
      transformeredNode = processSubQ(leftFB, rightFB);
    } else {
      throw new SqlXlateException("Not support complex sub query with and condition.");
    }
    fb.setTransformedNode(transformeredNode);
  }

  /**
   * merge uncorrelated filter condition to transformed subq AST.
   *
   * @param simpleFilter
   * @param subqFilter
   * @return
   */
  private CommonTree processUnCorrelated(FilterBlock simpleFilter, FilterBlock subqFilter) {
    CommonTree transformerdSubqSelect = subqFilter.getTransformedNode();
    CommonTree simpleCondition = simpleFilter.getASTNode();
    CommonTree topSelect = getTopSelect(transformerdSubqSelect);
    CommonTree where = (CommonTree) topSelect
        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_WHERE);
    if (where == null) {
      where = FilterBlockUtil.createSqlASTNode(PantheraExpParser.SQL92_RESERVED_WHERE, "where");
      topSelect.addChild(where);
      CommonTree logicExpr = FilterBlockUtil.createSqlASTNode(PantheraExpParser.LOGIC_EXPR,
          "LOGIC_EXPR");
      where.addChild(logicExpr);
      logicExpr.addChild(simpleCondition);
    } else {
      // TODO
      throw new RuntimeException("unimplement logic.");
    }
    return transformerdSubqSelect;
  }


  /**
   * merge left transformed subq AST to right transformed subq AST
   *
   * @deprecated
   * @param leftFB
   * @param rightFB
   * @return
   */

  @Deprecated
  private CommonTree processSubQ(FilterBlock leftFB, FilterBlock rightFB) {
    CommonTree leftSelect = leftFB.getTransformedNode();
    CommonTree rightSelect = rightFB.getTransformedNode();
    CommonTree rightTopSelect = getTopSelect(rightSelect);
    CommonTree rightTopSelectList = (CommonTree) rightTopSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    CommonTree rightTopSubQuery = (CommonTree) rightTopSelect.getParent();
    rightTopSubQuery.deleteChild(0);// delete right top select node
    rightTopSubQuery.addChild(leftSelect);
    CommonTree leftSelectList = (CommonTree) leftSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    for (int i = 0; i < rightTopSelectList.getChildCount(); i++) {
      CommonTree rightTopSelectItem = (CommonTree) rightTopSelectList.getChild(i);
      if (i >= leftSelectList.getChildCount()) {
        // leftSelectList.addChild(rightTopSelectItem);
      } else {
        // replace left SelectItem'alias by right top SelectItem
        CommonTree leftSelectItem = (CommonTree) leftSelectList.getChild(i);
        String alias = rightTopSelectItem.getChild(1).getChild(0).getText();
        ((CommonTree) leftSelectItem.getChild(1).getChild(0)).getToken().setText(alias);
      }
    }
    return rightSelect;
  }

  private CommonTree getTopSelect(CommonTree select) {
    CommonTree tableRef = (CommonTree) ((CommonTree) select
        .getFirstChildWithType(PantheraExpParser.SQL92_RESERVED_FROM))
        .getFirstChildWithType(PantheraExpParser.TABLE_REF);
    List<CommonTree> nodeList = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(tableRef, PantheraExpParser.SQL92_RESERVED_SELECT, nodeList);
    CommonTree topSelect = nodeList.get(0);
    return topSelect;
  }
}
