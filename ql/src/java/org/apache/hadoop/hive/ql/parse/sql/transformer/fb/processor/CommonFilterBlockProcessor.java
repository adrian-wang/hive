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

import java.util.List;

import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * provide common process logic for filter block processor.
 * TODO<br>
 * <li>processCompareHavingUC and processEqualsUC didn¡¯t check whether single-row expression
 * returns only one row. It¡¯s ok for these 2 cases because sum and max always return one value.
 * Just a reminder don¡¯t miss the check part. If you didn¡¯t implement yet, add a ¡°TODO¡± remark
 * in code.<br>
 *
 * <li>there are some duplicate code need to be refactor.
 *
 * CommonFilterBlockProcessor.
 *
 */

public abstract class CommonFilterBlockProcessor extends BaseFilterBlockProcessor {

  CommonTree topAlias;
  CommonTree bottomAlias;
  CommonTree topTableRefElement;
  List<CommonTree> topAliasList;
  CommonTree closingSelect;
  CommonTree join;

  /**
   * make branch for top select
   */
  private void makeTop() {

    // create top select table ref node
    topTableRefElement = super.createTableRefElement(topSelect);
    topAlias = (CommonTree) topTableRefElement.getChild(0);

    // add alias for all top query select item
    topAliasList = super.buildSelectListAlias(topAlias, (CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST));



    // create closing select
    closingSelect = super.createClosingSelect(topTableRefElement);
  }

  /**
   * make branch join
   *
   * @param joinType
   */
  private void makeJoin(CommonTree joinType) {
    // join
    join = super.createJoin(closingSelect);
    bottomAlias = super.buildJoin(joinType, join, bottomSelect);
  }

  /**
   * make branch for whole sub tree
   */
  private void makeEnd() {
    // closing select list
    CommonTree selectList = super.createSelectListForClosingSelect(topAliasList);
    FilterBlockUtil.attachChild(closingSelect, selectList);

    // set closing select to top select
    topSelect = closingSelect;
  }

  /**
   * process compare operator with correlated
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processCompareC(CommonTree joinType) throws SqlXlateException {

    this.makeTop();
    this.makeJoin(FilterBlockUtil.createSqlASTNode(PantheraParser_PLSQLParser.CROSS_VK, "cross"));

    CommonTree compareKeyAlias1 = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), (CommonTree) this.subQNode
        .getFirstChildWithType(PantheraParser_PLSQLParser.CASCATED_ELEMENT));


    // join
    // CommonTree join = this.createJoin(topSelect);
    // CommonTree joinSubAlias = super.buildJoin(super.createSqlASTNode(
    // PantheraParser_PLSQLParser.CROSS_VK, "cross"), join, bottomSelect);

    // select list
    CommonTree compareKeyAlias2 = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));// childCount==0;
    super.rebuildSelectListByFilter(true, bottomAlias, topAlias);

    // TODO cross join optimization will do it
    // on
    // CommonTree on = super.buildOn(FilterBlockUtil.dupNode(this.fb.getASTNode()), FilterBlockUtil
    // .cloneTree((CommonTree) joinKey[1]), super.createCascatedElementWithTableName(
    // (CommonTree) joinSubAlias.getChild(0), FilterBlockUtil.cloneTree((CommonTree) joinKeyAlias
    // .getChild(0))));
    // super.attachChild(join, on);

    this.makeEnd();

    // where
    super.buildWhereByFB(subQNode, compareKeyAlias1, compareKeyAlias2);

  }


  /**
   * process exists with correlated
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processExistsC(CommonTree joinType) throws SqlXlateException {
    this.makeTop();


    this.makeJoin(FilterBlockUtil.createSqlASTNode(PantheraParser_PLSQLParser.CROSS_VK,
        PantheraExpParser.LEFTSEMI_STR));


    super.processSelectAsterisk(bottomSelect);
    super.rebuildSelectListByFilter(false, bottomAlias, topAlias);

    // // delete where
    // super.deleteBranch(bottomSelect, PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
    //
    // // on
    // CommonTree on = super.buildOn(FilterBlockUtil.dupNode(fb.getASTNode()), super
    // .rebuildCascatedElement((CommonTree) fb.getASTNode().getChild(0)), super
    // .rebuildCascatedElement((CommonTree) fb.getASTNode().getChild(1)));
    // super.attachChild(join, on);


    this.makeEnd();
    super.buildWhereByFB(null, null, null);
  }

  /**
   * process compare operator in having clause with uncorrelated
   *
   * @param joinType
   */
  void processCompareHavingUC(CommonTree joinType) {

    // delete having. DO it before clone.
    FilterBlockUtil.deleteBranch((CommonTree) topQuery.getASTNode()
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP),
        PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING);

    // needn't group after transformed.
    super.topQuery.setGroup(null);

    // clone whole top select tree
    super.topSelect = super.topQuery.cloneWholeQuery();

    // create top select table ref node
    topTableRefElement = super.createTableRefElement(topSelect);
    topAlias = (CommonTree) topTableRefElement.getChild(0);

    // add alias for all top query select item
    List<CommonTree> topAliasList = super.buildSelectListAlias(topAlias, (CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST));

    // add compare item
    CommonTree compareElementAlias = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), super.cloneSubQOpElement());


    // create closing select
    CommonTree closingSelect = super.createClosingSelect(topTableRefElement);

    // join
    CommonTree join = super.createJoin(closingSelect);
    CommonTree subAlias = super.buildJoin(joinType, join, bottomSelect);

    // compare alias from subq
    CommonTree comparSubqAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));

    // closing select list
    CommonTree selectList = super.createSelectListForClosingSelect(topAliasList);
    FilterBlockUtil.attachChild(closingSelect, selectList);

    // where
    // FIXME which is first?
    CommonTree where = super.buildWhere(FilterBlockUtil.dupNode(subQNode), super
        .createCascatedElementWithTableName(
            (CommonTree) topTableRefElement.getChild(0).getChild(0),
            (CommonTree) compareElementAlias.getChild(0)), super
        .createCascatedElementWithTableName((CommonTree) subAlias.getChild(0),
            (CommonTree) comparSubqAlias.getChild(0)));
    FilterBlockUtil.attachChild(closingSelect, where);

    // set closing select to top select
    topSelect = closingSelect;
  }

  /**
   * process compare operator with uncorrelated
   *
   * @param joinType
   */
  void processCompareUC(CommonTree joinType) {

    // must have aggregation function in sub query
    this.processAggregationCompareUC();
  }

  private void processAggregationCompareUC() {


    this.makeTop();

    // add compare item
    CommonTree compareElement = super.getSubQOpElement();
    CommonTree cloneCompareElement = FilterBlockUtil.cloneTree(compareElement);
    CommonTree compareElementAlias = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), cloneCompareElement);
    super.rebuildSubQOpElement(compareElement, compareElementAlias);



    this.makeJoin(FilterBlockUtil.createSqlASTNode(PantheraParser_PLSQLParser.CROSS_VK, "cross"));

    // compare alias from subq
    CommonTree comparSubqAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));

    this.makeEnd();

    // where
    super.buildWhereBranch(bottomAlias, comparSubqAlias);


  }

  /**
   * process in with uncorrelated
   *
   * @param joinType
   */
  void processInUC(CommonTree joinType) {

    this.makeTop();

    // add compare item
    // TODO multi parameter in sub query IN
    CommonTree compareElementAlias = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), super.cloneSubQOpElement());

    this.makeJoin(joinType);

    // compare alias from subq
    CommonTree comparSubqAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));

    // on
    // FIXME which is first?
    CommonTree on = super.buildOn(
        FilterBlockUtil.createSqlASTNode(PantheraParser_PLSQLParser.EQUALS_OP, "="), super
            .createCascatedElementWithTableName((CommonTree) topAlias.getChild(0),
                (CommonTree) compareElementAlias.getChild(0)), super
            .createCascatedElementWithTableName((CommonTree) bottomAlias.getChild(0),
                (CommonTree) comparSubqAlias.getChild(0)));
    FilterBlockUtil.attachChild(join, on);

    this.makeEnd();
  }

  /**
   * process not in with uncorrelated
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processNotInUC() throws SqlXlateException {
    CommonTree joinType = FilterBlockUtil.createSqlASTNode(PantheraParser_PLSQLParser.CROSS_VK, "cross");
    this.makeTop();


    // add compare item
    List<CommonTree> compareElementAlias = super.addSelectItems4In(topSelect, super.subQNode);


    this.makeJoin(joinType);
    super.rebuildCollectSet();

    // compare alias from subq
    List<CommonTree> comparSubqAlias = super.buildSelectListAlias(bottomAlias,
        (CommonTree) bottomSelect
            .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST));


    this.makeEnd();

    // where
    // FIXME which is first?
    CommonTree where = super.buildWhere(FilterBlockUtil.dupNode(subQNode), comparSubqAlias,
        compareElementAlias);
    FilterBlockUtil.attachChild(closingSelect, where);
    super.rebuildArrayContains((CommonTree) where.getChild(0));
  }

  /**
   * process not exists with correlated
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processNotExistsC(CommonTree joinType) throws SqlXlateException {
    // minuend tree
    CommonTree minuendSelect = FilterBlockUtil.cloneTree(topSelect);

    this.processExistsC(joinType);

    // not in
    super.subQNode = super.buildNotIn4Minus(minuendSelect, topSelect);
    bottomSelect = topSelect;
    topSelect = minuendSelect;

    this.processNotInUC();


  }

  void processAnd(CommonTree joinType) {
    this.makeTop();
    this.makeJoin(joinType);
    CommonTree topSelectList = (CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    CommonTree bottomSelectList = (CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    // on
    CommonTree on = super.makeOn(topSelectList, bottomSelectList, topAlias, bottomAlias);
    FilterBlockUtil.attachChild(join, on);

    this.makeEnd();
  }
}
