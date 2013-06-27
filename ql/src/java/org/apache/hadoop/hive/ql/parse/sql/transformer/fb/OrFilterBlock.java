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

import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * transform OR to UNION
 * OrFilterBlock.
 *
 */
public class OrFilterBlock extends LogicFilterBlock {

  @Override
  public void process(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    super.processChildren(fbContext, context);
    CommonTree leftSelect = this.getChildren().get(0).getTransformedNode();
    CommonTree rightSelect = this.getChildren().get(1).getTransformedNode();
    CommonTree topSelect = this.buildUnionSelect(leftSelect, rightSelect);
    CommonTree distinct = FilterBlockUtil.createSqlASTNode(PantheraExpParser.SQL92_RESERVED_DISTINCT, "distinct");
    topSelect.addChild(distinct);
    CommonTree leftSelectList = (CommonTree) leftSelect.getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    CommonTree selectList;
    if(leftSelectList!=null){
       selectList = FilterBlockUtil.cloneTree(leftSelectList);
       rebuildSelectAlias(selectList);
    }else{
      selectList = FilterBlockUtil.createSqlASTNode(PantheraExpParser.ASTERISK, "*");
    }
    topSelect.addChild(selectList);
    this.setTransformedNode(topSelect);
  }

  private CommonTree buildUnionSelect(CommonTree leftSelect, CommonTree rightSelect) {
    CommonTree union = FilterBlockUtil.createSqlASTNode(PantheraExpParser.SQL92_RESERVED_UNION,
        "union");
    union.addChild(rightSelect);
    CommonTree subquery = FilterBlockUtil.createSqlASTNode(PantheraExpParser.SUBQUERY, "SUBQUERY");
    subquery.addChild(leftSelect);
    subquery.addChild(union);
    CommonTree topSelect = FilterBlockUtil.createSqlASTNode(
        PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    CommonTree from = FilterBlockUtil.createSqlASTNode(
        PantheraParser_PLSQLParser.SQL92_RESERVED_FROM, "from");
    FilterBlockUtil.attachChild(topSelect, from);
    CommonTree tableRef = FilterBlockUtil.createSqlASTNode(PantheraParser_PLSQLParser.TABLE_REF,
        "TABLE_REF");
    FilterBlockUtil.attachChild(from, tableRef);
    CommonTree tableRefElement = FilterBlockUtil.createSqlASTNode(
        PantheraParser_PLSQLParser.TABLE_REF_ELEMENT, "TABLE_REF_ELEMENT");
    FilterBlockUtil.attachChild(tableRef, tableRefElement);
    CommonTree tableExpression = FilterBlockUtil.createSqlASTNode(
        PantheraParser_PLSQLParser.TABLE_EXPRESSION, "TABLE_EXPRESSION");
    FilterBlockUtil.attachChild(tableRefElement, tableExpression);
    CommonTree selectMode = FilterBlockUtil.createSqlASTNode(
        PantheraParser_PLSQLParser.SELECT_MODE, "SELECT_MODE");
    FilterBlockUtil.attachChild(tableExpression, selectMode);
    CommonTree selectStatement = FilterBlockUtil.createSqlASTNode(
        PantheraParser_PLSQLParser.SELECT_STATEMENT, "SELECT_STATEMENT");
    FilterBlockUtil.attachChild(selectMode, selectStatement);
    selectStatement.addChild(subquery);

    return topSelect;
  }

  private void rebuildSelectAlias(CommonTree selectList){
    for(int i=0;i<selectList.getChildCount();i++){
      CommonTree selectItem =(CommonTree) selectList.getChild(i);
      if(selectItem.getChildCount()==2){
        String alias = selectItem.getChild(1).getChild(0).getText();
        CommonTree column = (CommonTree) selectItem.getChild(0).getChild(0).getChild(0).getChild(0);
        column.getToken().setText(alias);
      }
    }
  }
}
