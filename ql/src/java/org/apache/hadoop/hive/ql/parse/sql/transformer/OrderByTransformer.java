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

import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.SqlASTNode;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * support order by 1,2
 * OrderByTransformer.
 *
 */
public class OrderByTransformer extends BaseSqlASTTransformer {

  SqlASTTransformer tf;

  public OrderByTransformer(SqlASTTransformer tf) {
    this.tf = tf;
  }

  @Override
  public void transform(SqlASTNode tree, TranslateContext context) throws SqlXlateException {
    tf.transformAST(tree, context);
    CommonTree selectStatement = (CommonTree) tree
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_STATEMENT);
    if (selectStatement != null) {
      CommonTree order = (CommonTree) selectStatement
          .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_ORDER);
      if (order != null) {
        CommonTree selectList = (CommonTree) ((CommonTree) ((CommonTree) selectStatement
            .getFirstChildWithType(PantheraParser_PLSQLParser.SUBQUERY))
            .getFirstChildWithType(PantheraParser_PLSQLParser.SQL92_RESERVED_SELECT))
            .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST);
        if(selectList==null){
          //? select *
          return;
        }
        CommonTree orderByElements = (CommonTree) order.getChild(0);
        for (int i = 0; i < orderByElements.getChildCount(); i++) {
          CommonTree ct = (CommonTree) orderByElements.getChild(i).getChild(0).getChild(0);
          if (ct.getType() == PantheraParser_PLSQLParser.UNSIGNED_INTEGER) {// order by 1,2
            CommonTree expr = (CommonTree) orderByElements.getChild(i).getChild(0);
            int seq = Integer.valueOf(ct.getText());
            CommonTree selectItem = (CommonTree) selectList.getChild(seq-1);
            String colName;
            if(selectItem.getChildCount()==2){
              colName = selectItem.getChild(1).getChild(0).getText();
            }else{
              colName = selectItem.getChild(0).getChild(0).getChild(0).getChild(0).getText();
            }
            CommonTree cascatedElement = SqlXlateUtil.newSqlASTNode(PantheraParser_PLSQLParser.CASCATED_ELEMENT, "CASCATED_ELEMENT");
            CommonTree anyElement = SqlXlateUtil.newSqlASTNode(PantheraParser_PLSQLParser.ANY_ELEMENT, "ANY_ELEMENT");
            CommonTree col = SqlXlateUtil.newSqlASTNode(PantheraParser_PLSQLParser.ID, colName);
            expr.deleteChild(0);
            expr.addChild(cascatedElement);
            cascatedElement.addChild(anyElement);
            anyElement.addChild(col);
          }
        }
      }
    }
  }
}
