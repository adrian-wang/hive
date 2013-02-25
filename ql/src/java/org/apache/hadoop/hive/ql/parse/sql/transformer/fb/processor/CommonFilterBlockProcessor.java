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
    CommonTree selectList = super.createSelectList(topAliasList);
    super.attachChild(closingSelect, selectList);

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
    // join
    CommonTree join = this.createJoin(topSelect);
    CommonTree joinSubAlias = super.buildJoin(joinType, join, bottomSelect);

    // select list
    CommonTree compareKeyAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));
    CommonTree[] joinKey = super.getJoinKey();
    CommonTree joinKeyAlias = super.addSelectItem((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), FilterBlockUtil
        .cloneTree(joinKey[0]));

    // on
    CommonTree on = super.buildOn(FilterBlockUtil.dupNode(this.fb.getASTNode()), FilterBlockUtil
        .cloneTree((CommonTree) joinKey[1]), super.createCascatedElementWithTableName(
        (CommonTree) joinSubAlias.getChild(0), FilterBlockUtil.cloneTree((CommonTree) joinKeyAlias
            .getChild(0))));
    super.attachChild(join, on);

    // group
    super.buildGroup(FilterBlockUtil.cloneTree(joinKey[0]));

    // where
    CommonTree where = super.buildWhere(FilterBlockUtil.dupNode(super.subQNode),
        (CommonTree) super.subQNode
            .getFirstChildWithType(PantheraParser_PLSQLParser.CASCATED_ELEMENT), super
            .createCascatedElement(FilterBlockUtil
                .cloneTree((CommonTree) compareKeyAlias.getChild(0))));
    super.attachChild(topSelect, where);

    // TODO naive, simple
    super.deleteBranch(bottomSelect, PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);
  }

  /**
   * process equals operator with uncorrelated
   *
   * @param joinType
   */
  void processEqualsUC(CommonTree joinType) {
    this.makeTop();

    // add compare item
    CommonTree compareElementAlias = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), super.cloneSubQOpElement());


    this.makeJoin(joinType);

    // compare alias from subq
    CommonTree comparSubqAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));

    // on
    // FIXME which is first?
    CommonTree on = super.buildOn(FilterBlockUtil.dupNode(subQNode), super
        .createCascatedElementWithTableName((CommonTree) topAlias.getChild(0),
            (CommonTree) compareElementAlias.getChild(0)), super.createCascatedElement(
        (CommonTree) bottomAlias.getChild(0), (CommonTree) comparSubqAlias.getChild(0)));
    super.attachChild(join, on);

    this.makeEnd();
  }

  /**
   * process exists with correlated
   *
   * @param joinType
   */
  void processExistsC(CommonTree joinType) {
    this.makeTop();

    this.makeJoin(joinType);

    // delete where
    super.deleteBranch(bottomSelect, PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);

    // on
    CommonTree on = super.buildOn(FilterBlockUtil.dupNode(fb.getASTNode()), super
        .rebuildCascatedElement((CommonTree) fb.getASTNode().getChild(0)), super
        .rebuildCascatedElement((CommonTree) fb.getASTNode().getChild(1)));
    super.attachChild(join, on);

    this.makeEnd();

  }

  /**
   * process compare operator in having clause with uncorrelated
   *
   * @param joinType
   */
  void processCompareHavingUC(CommonTree joinType) {

    // clone whole top select tree
    super.topSelect = super.topQuery.cloneWholeQuery();

    // delete having
    super.deleteBranch((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_GROUP),
        PantheraParser_PLSQLParser.SQL92_RESERVED_HAVING);

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
    CommonTree selectList = super.createSelectList(topAliasList);
    super.attachChild(closingSelect, selectList);

    // where
    // FIXME which is first?
    CommonTree where = super.buildWhere(FilterBlockUtil.dupNode(subQNode), super
        .createCascatedElementWithTableName(
            (CommonTree) topTableRefElement.getChild(0).getChild(0),
            (CommonTree) compareElementAlias.getChild(0)), super
        .createCascatedElementWithTableName((CommonTree) subAlias.getChild(0),
            (CommonTree) comparSubqAlias.getChild(0)));
    super.attachChild(closingSelect, where);

    // set closing select to top select
    topSelect = closingSelect;
  }

  /**
   * process compare operator with uncorrelated
   *
   * @param joinType
   */
  void processCompareUC(CommonTree joinType) {

    if (subQNode.getChild(1).getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_ALL) {// >all

      // join
      CommonTree join = this.createJoin(topSelect);
      CommonTree viewAlias = super.buildJoin(joinType, join, bottomSelect);

      // max
      CommonTree selectItem = (CommonTree) bottomSelect
          .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST).getChild(0);
      CommonTree expr = (CommonTree) selectItem.getChild(0);
      CommonTree function = super.createFunction("max", (CommonTree) expr.deleteChild(0));
      super.attachChild(expr, function);
      CommonTree maxAlias = super.createAlias();
      super.attachChild(selectItem, maxAlias);

      // where
      super.buildWhereBranch(viewAlias, maxAlias);
    }
  }

  /**
   * process in with uncorrelated
   *
   * @param joinType
   */
  void processInUC(CommonTree joinType) {

    this.makeTop();

    // add compare item
    CommonTree compareElementAlias = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), super.cloneSubQOpElement());

    this.makeJoin(joinType);

    // compare alias from subq
    CommonTree comparSubqAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));

    // on
    // FIXME which is first?
    CommonTree on = super.buildOn(
        super.createSqlASTNode(PantheraParser_PLSQLParser.EQUALS_OP, "="), super
            .createCascatedElementWithTableName((CommonTree) topAlias.getChild(0),
                (CommonTree) compareElementAlias.getChild(0)), super
            .createCascatedElementWithTableName((CommonTree) bottomAlias.getChild(0),
                (CommonTree) comparSubqAlias.getChild(0)));
    super.attachChild(join, on);

    this.makeEnd();
  }

  /**
   * process not in with uncorrelated
   *
   * @param joinType
   * @throws SqlXlateException
   */
  void processNotInUC(CommonTree joinType) throws SqlXlateException {

    this.makeTop();


    // add compare item
    CommonTree compareElementAlias = super.addSelectItem((CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST), super.cloneSubQOpElement());


    this.makeJoin(joinType);
    super.rebuildCollectSet();

    // compare alias from subq
    CommonTree comparSubqAlias = super.addAlias((CommonTree) ((CommonTree) bottomSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST)).getChild(0));


    this.makeEnd();

    // where
    // FIXME which is first?
    CommonTree where = super.buildWhere(FilterBlockUtil.dupNode(subQNode), super
        .createCascatedElement((CommonTree) comparSubqAlias.getChild(0)), super
        .createCascatedElement((CommonTree) compareElementAlias.getChild(0)));
    super.attachChild(closingSelect, where);
    super.rebuildArrayContains((CommonTree) where.getChild(0));
  }

  /**
   * process not exists with correlated
   *
   * @param joinType
   */
  void processNotExistsC(CommonTree joinType) {

    // minuend tree
    CommonTree minuend = FilterBlockUtil.cloneTree(topSelect);

    // create minuend select table ref node
    CommonTree tableRef = super.createTableRefElement(minuend);
    topAlias = (CommonTree) tableRef.getChild(0);


    // create closing select
    closingSelect = super.createClosingSelect(tableRef);

    // minus
    super.attachChild(tableRef, super.createMinus(topSelect));

    // join
    CommonTree join = super.createJoin(topSelect);
    super.buildJoin(joinType, join, bottomSelect);


    // delete where
    super.deleteBranch(bottomSelect, PantheraParser_PLSQLParser.SQL92_RESERVED_WHERE);

    // on
    CommonTree on = super.buildOn(FilterBlockUtil.dupNode(fb.getASTNode()), (CommonTree) fb
        .getASTNode().getChild(0), (CommonTree) fb.getASTNode().getChild(1));
    super.attachChild(join, on);


    // add alias for all top query select item
    topAliasList = super.buildSelectListAlias(topAlias, (CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST));

    this.makeEnd();



  }
}
