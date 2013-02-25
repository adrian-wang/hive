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

import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.QueryBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.SubQFilterBlock;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * Process uncorrelated filter block node which subQ is GREATER_THAN type.
 * GreaterThanProcessor4UC.
 *
 */
public class GreaterThanProcessor4UC extends BaseFilterBlockProcessor {

  @Override
  public void process(FilterBlockContext fbContext, FilterBlock fb, TranslateContext context) {

    QueryBlock bottomQuery = fbContext.getQueryStack().pop();
    QueryBlock topQuery = fbContext.getQueryStack().peek();
    fbContext.getQueryStack().push(bottomQuery);
    SubQFilterBlock subQ = fbContext.getSubQStack().peek();
    CommonTree topSelect = topQuery.getASTNode();// maybe need clone.
    CommonTree bottomSelect = bottomQuery.getASTNode();
    CommonTree subQNode = subQ.getASTNode();

    if (subQNode.getChild(1).getType() == PantheraParser_PLSQLParser.SQL92_RESERVED_ALL) {// >all
      // join
      CommonTree from = (CommonTree) topSelect
          .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_FROM);
      CommonTree join = super.createSqlASTNode(PantheraParser_PLSQLParser.JOIN_DEF, "join");
      super.attachChild((CommonTree) from.getChild(0), join);
      super.attachChild(join, super.createSqlASTNode(PantheraParser_PLSQLParser.CROSS_VK, "cross"));
      CommonTree tableRefElement = super.createSqlASTNode(
          PantheraParser_PLSQLParser.TABLE_REF_ELEMENT, "TABLE_REF_ELEMENT");
      super.attachChild(join, tableRefElement);
      CommonTree viewAlias = super.createAlias(context);
      tableRefElement.addChild(viewAlias);
      tableRefElement.addChild(super.createSubQ(bottomSelect));

      // max
      CommonTree selectItem = (CommonTree) bottomSelect
          .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST).getChild(0);
      CommonTree expr = (CommonTree) selectItem.getChild(0);
      CommonTree function = super.createFunction("max", (CommonTree) expr.deleteChild(0));
      super.attachChild(expr, function);
      CommonTree maxAlias = super.createAlias(context);
      super.attachChild(selectItem, maxAlias);

      // opParameter
      subQNode.deleteChild(1);
      super.attachChild(subQNode, super.createOpParameter(viewAlias, maxAlias));

    }
    fb.setTransformedNode(topSelect);

  }

}
