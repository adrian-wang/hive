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
import java.util.List;

import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * transform and to intersect
 * AndFilterBlock.
 *
 */
public class AndFilterBlock extends LogicFilterBlock {

  /**
   * this must have two children.
   *
   * @throws SqlXlateException
   */
  @Override
  public void process(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    // super.processChildren(fbContext, context);
    // FilterBlockProcessorFactory.getAndProcessor().process(fbContext, this, context);
    FilterBlock leftFB = this.getChildren().get(0);
    leftFB.process(fbContext, context);
    fbContext.getQueryStack().peek().setQueryForTransfer(leftFB.getTransformedNode());
    FilterBlock rightFB = this.getChildren().get(1);
    rightFB.process(fbContext, context);
    CommonTree logicTopSelect = fbContext.getLogicTopSelect();
    this.rebuildSelectList(logicTopSelect);
    this.setTransformedNode(rightFB.getTransformedNode());
  }

  /**
   * add select item to the most left select
   *
   * @param select
   */
  private void rebuildSelectList(CommonTree select) {
    CommonTree outerSelectList = (CommonTree) select
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
    List<CommonTree> nodeList = new ArrayList<CommonTree>();
    FilterBlockUtil.findNode(select, PantheraExpParser.SELECT_LIST, nodeList);
    if (!nodeList.isEmpty()) {
      CommonTree innerSelectList = nodeList.get(0);
      for (int i = 0; i < outerSelectList.getChildCount(); i++) {
        CommonTree anyElement = (CommonTree) outerSelectList.getChild(i).getChild(0).getChild(0)
            .getChild(0);
        String columnName;
        if (anyElement.getChildCount() == 2) {
          columnName = anyElement.getChild(1).getText();
        } else {
          columnName = anyElement.getChild(0).getText();
        }
        List<CommonTree> nameList = new ArrayList<CommonTree>();
        CommonTree selectItem = null;
        boolean isExist = false;
        for (int j = 0; j < innerSelectList.getChildCount(); j++) {
          selectItem = (CommonTree) innerSelectList.getChild(j);
          FilterBlockUtil.findNodeText(selectItem, columnName, nameList);
          if (!nameList.isEmpty()) {
            isExist = true;
            break;
          }
        }
        if (!isExist) {
          CommonTree newSelectItem = FilterBlockUtil.cloneTree((CommonTree) outerSelectList
              .getChild(i));
          newSelectItem.deleteChild(1);
          innerSelectList.addChild(newSelectItem);
        } else {
          if (selectItem.getChildCount() == 2) {
            String alias = selectItem.getChild(1).getChild(0).getText();
            for (int k = 0; k < anyElement.getChildCount(); k++) {
              anyElement.deleteChild(k);
            }
            CommonTree newColumn = FilterBlockUtil.createSqlASTNode(PantheraExpParser.ID, alias);
            anyElement.addChild(newColumn);
          }
        }
      }
    }
  }
}
