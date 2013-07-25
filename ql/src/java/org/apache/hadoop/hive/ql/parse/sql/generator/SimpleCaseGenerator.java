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
package org.apache.hadoop.hive.ql.parse.sql.generator;

import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlParseException;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateUtil;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

/**
 * Generator for case node.
 *
 * This generator handles the syntax:
 *    CASE value WHEN [compare_value] THEN result [WHEN [compare_value] THEN result ...] [<ELSE result] END
 *
 */
public class SimpleCaseGenerator extends BaseHiveASTGenerator {

  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws Exception {
    ASTNode ret = super.newHiveASTNode(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
    super.attachHiveNode(hiveRoot, currentHiveNode, ret);
    //transfer to standard CASE WHEN expression THEN compval [WHEN expression THEN compval]* [ELSE result] END
    ASTNode whenNode = super.newHiveASTNode(HiveParser.KW_WHEN, "when");
    super.attachHiveNode(hiveRoot, ret, whenNode);
    if (!(currentSqlNode.getChildCount()>=3)) {
      throw new SqlParseException("Parameters error in simple case expression.");
    }
    if (!(currentSqlNode.getChild(0) instanceof org.antlr33.runtime.tree.CommonTree)) {
      throw new SqlParseException("illegal sql AST node:" + currentSqlNode.getChild(0));
    }
    for (int i = 1; i < currentSqlNode.getChildCount();i++) {
      if (!(currentSqlNode.getChild(i) instanceof org.antlr33.runtime.tree.CommonTree)) {
        throw new SqlParseException("illegal sql AST node:" + currentSqlNode.getChild(i));
      }
      CommonTree node = (CommonTree) currentSqlNode.getChild(i);
      if (node.getChildCount()==1) {
        //else clause
        HiveASTGenerator generator = GeneratorFactory.getGenerator(node);
        if (generator == null) {
          throw new SqlParseException("illegal sql AST node:" + currentSqlNode.getChild(i));
        }
        if (!generator.generateHiveAST(hiveRoot, sqlRoot,
            ret, node, context)) {
          return false;
        }
      } else {
        //when-then clause
        ASTNode equal = SqlXlateUtil.newASTNode(HiveParser.EQUAL, "=");
        SqlXlateUtil.attachChild(ret, equal);
        HiveASTGenerator generatorL = GeneratorFactory.getGenerator((CommonTree) currentSqlNode.getChild(0));
        if (generatorL == null) {
          throw new SqlParseException("illegal sql AST node:" + currentSqlNode.getChild(0));
        }
        if (!generatorL.generateHiveAST(hiveRoot, sqlRoot,
            equal, (CommonTree) currentSqlNode.getChild(0), context)) {
          return false;
        }
        HiveASTGenerator generatorR = GeneratorFactory.getGenerator((CommonTree)node.getChild(0));
        if (generatorR == null) {
          throw new SqlParseException("illegal sql AST node:" + node.getChild(0));
        }
        if (!generatorR.generateHiveAST(hiveRoot, sqlRoot,
            equal, (CommonTree)node.getChild(0), context)) {
          return false;
        }
        HiveASTGenerator resultgen = GeneratorFactory.getGenerator((CommonTree)node.getChild(1));
        if (resultgen == null) {
          throw new SqlParseException("illegal sql AST node:" + node.getChild(1));
        }
        if (!resultgen.generateHiveAST(hiveRoot, sqlRoot,
            ret, (CommonTree)node.getChild(1), context)) {
          return false;
        }
      }
    }
    return true;
  }

}
