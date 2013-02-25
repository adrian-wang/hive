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

import org.antlr33.runtime.CommonToken;
import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;


public abstract class BaseFilterBlockProcessor implements FilterBlockProcessor {

  void attachChild(CommonTree parent, CommonTree child) {
    if (parent != null) {
      parent.addChild(child);
      if (child != null) {
        child.setParent(parent);
      }
    }
  }

  CommonTree createSqlASTNode(int type, String text) {
    return new CommonTree(new CommonToken(type, text));
  }

  CommonTree createAlias(TranslateContext context) {
    CommonTree alias = createSqlASTNode(PantheraParser_PLSQLParser.ALIAS, "ALIAS");
    CommonTree aliasName = createSqlASTNode(PantheraParser_PLSQLParser.ID, context.getAliasGen()
        .generateAliasName());
    this.attachChild(alias, aliasName);
    return alias;
  }

  CommonTree createSubQ(CommonTree selectNode) {
    CommonTree tableExpression = this.createSqlASTNode(PantheraParser_PLSQLParser.TABLE_EXPRESSION,
        "TABLE_EXPRESSION");
    CommonTree selectMode = this.createSqlASTNode(PantheraParser_PLSQLParser.SELECT_MODE,
        "SELECT_MODE");
    this.attachChild(tableExpression, selectMode);
    CommonTree selectStatement = this.createSqlASTNode(PantheraParser_PLSQLParser.SELECT_STATEMENT,
        "SELECT_STATEMENT");
    this.attachChild(selectMode, selectStatement);
    CommonTree subQuery = this.createSqlASTNode(PantheraParser_PLSQLParser.SUBQUERY, "SUBQUERY");
    this.attachChild(selectStatement, subQuery);
    this.attachChild(subQuery, selectNode);
    return tableExpression;
  }

  CommonTree createFunction(String functionName, CommonTree element) {
    CommonTree standardFunction = this.createSqlASTNode(
        PantheraParser_PLSQLParser.STANDARD_FUNCTION, "STANDARD_FUNCTION");
    CommonTree function = this.createSqlASTNode(PantheraParser_PLSQLParser.ID, functionName);
    this.attachChild(standardFunction, function);
    CommonTree arguments = this.createSqlASTNode(PantheraParser_PLSQLParser.ARGUMENTS, "ARGUMENTS");
    this.attachChild(function, arguments);
    CommonTree argument = this.createSqlASTNode(PantheraParser_PLSQLParser.ARGUMENT, "ARGUMENT");
    this.attachChild(arguments, argument);
    CommonTree expr = this.createSqlASTNode(PantheraParser_PLSQLParser.EXPR, "EXPR");
    this.attachChild(argument, expr);
    this.attachChild(expr, element);
    return standardFunction;
  }

  CommonTree createOpParameter(CommonTree viewAlias, CommonTree colAlias) {
    CommonTree cascatedElement = this.createSqlASTNode(PantheraParser_PLSQLParser.CASCATED_ELEMENT,
        "CASCATED_ELEMENT");
    CommonTree anyElement = this.createSqlASTNode(PantheraParser_PLSQLParser.ANY_ELEMENT,
        "ANY_ELEMENT");
    this.attachChild(cascatedElement, anyElement);
    this.attachChild(anyElement, (CommonTree) viewAlias.getChild(0));
    this.attachChild(anyElement, (CommonTree) colAlias.getChild(0));
    return cascatedElement;
  }
}
