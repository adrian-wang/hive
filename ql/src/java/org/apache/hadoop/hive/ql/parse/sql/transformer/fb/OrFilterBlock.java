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
 * transform OR to UNION OrFilterBlock.
 *
 */
public class OrFilterBlock extends LogicFilterBlock {

  @Override
  public void process(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    super.processChildren(fbContext, context);
    CommonTree leftSelect = this.getChildren().get(0).getTransformedNode();
    CommonTree rightSelect = this.getChildren().get(1).getTransformedNode();
    CommonTree topSelect = this.buildUnionSelect(leftSelect, rightSelect, context);

    // UNION transformer will process DISTINCT
    // CommonTree distinct = FilterBlockUtil.createSqlASTNode(
    // PantheraExpParser.SQL92_RESERVED_DISTINCT, "distinct");
    // topSelect.addChild(distinct);

    CommonTree leftSelectList = (CommonTree) leftSelect
        .getFirstChildWithType(PantheraExpParser.SELECT_LIST);
    topSelect.addChild(FilterBlockUtil.cloneSelectList(leftSelectList));
    this.setTransformedNode(topSelect);
  }

  private CommonTree buildUnionSelect(CommonTree leftSelect, CommonTree rightSelect,
      TranslateContext context) {
    CommonTree union = FilterBlockUtil.createSqlASTNode(PantheraExpParser.SQL92_RESERVED_UNION,
        "union");
    union.addChild(rightSelect);
    CommonTree topSelect = FilterBlockUtil.createSqlASTNode(
        PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT, "select");
    CommonTree subquery = FilterBlockUtil.makeSelectBranch(topSelect, context);
    subquery.addChild(leftSelect);
    subquery.addChild(union);
    return topSelect;
  }


}
