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
package org.apache.hadoop.hive.ql.parse.sql;

import junit.framework.TestCase;

import org.antlr33.runtime.ANTLRStringStream;
import org.antlr33.runtime.TokenRewriteStream;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.sql.SqlParseDriver.SqlLexer;
import org.apache.hadoop.hive.ql.parse.sql.SqlParseDriver.SqlParser;
import org.apache.hadoop.hive.ql.parse.sql.transformer.NothingTransformer;
import org.apache.hadoop.hive.ql.parse.sql.transformer.PrepareFilterBlockTransformer;
import org.apache.hadoop.hive.ql.parse.sql.transformer.PrepareQueryInfoTransformer;
import org.apache.hadoop.hive.ql.parse.sql.transformer.SubQUnnestTransformer;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public abstract class BaseSQLTest extends TestCase {

  SqlParseDriver dr = new SqlParseDriver(null);
  SqlASTTranslator tr = new SqlASTTranslator(null);
  PrepareQueryInfoTransformer qt = new PrepareQueryInfoTransformer(new NothingTransformer());
  PrepareFilterBlockTransformer fbpt = new PrepareFilterBlockTransformer(qt);
  SubQUnnestTransformer fbt=new SubQUnnestTransformer(fbpt);


  SqlASTNode buildAST(String sql) {

    SqlLexer lexer = dr.new SqlLexer(new ANTLRStringStream(sql));

    TokenRewriteStream tokens = new TokenRewriteStream(lexer);

    SqlParser parser = dr.new SqlParser(tokens);
    parser.setTreeAdaptor(SqlParseDriver.adaptor);

    PantheraParser_PLSQLParser.seq_of_statements_return r = null;

    try {
      r = parser.seq_of_statements();
    } catch (org.antlr33.runtime.RecognitionException e) {
      //
    }
    SqlASTNode sqlAST = (SqlASTNode) r.getTree();
    this.printTreeType3(sqlAST);
    return sqlAST;
  }

  void testGenerateText(String sql, String ast) {
    this.testGenerate(sql, ast, true);
  }

  void testGenerateType(String sql, String ast) {
    this.testGenerate(sql, ast, false);
  }


  void testGenerate(String sql, String ast, boolean isText) {

    System.out.println(sql);
    SqlASTNode sqlAST = buildAST(sql);
    try {
      tr.setTransformer(new NothingTransformer());
      ASTNode hiveAST = tr.translate(sqlAST);
      System.out.println(hiveAST.toStringTree());
      this.printTreeType(hiveAST);
      if (isText) {
        assertEquals(ast, hiveAST.toStringTree());
      } else {
        assertEquals(ast, this.toTypeString(hiveAST));
      }
    } catch (SqlXlateException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }

  void testQInfo(String sql, String qTree) throws SqlXlateException {
    TranslateContext context = new TranslateContext(null);
    SqlASTNode sqlAST = buildAST(sql);
    try {
      qt.transform(sqlAST, context);
      String qtStree = context.getQInfoRoot().toStringTree();
      System.out.println(qtStree);
      assertEquals(qTree, qtStree);
    } catch (SqlXlateException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }

  void testPrepareFilterBlock(String sql, String fbTree) throws SqlXlateException {
    TranslateContext context = new TranslateContext(null);
    SqlASTNode sqlAST = buildAST(sql);
    try {
      fbpt.transform(sqlAST, context);
      String fbs= context.getQInfoRoot().toFilterBlockStringTree();
      System.out.println(fbs);
      assertEquals(fbTree, fbs);
    } catch (SqlXlateException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  void testFilterBlockTransformer(String sql, String fbTree) throws SqlXlateException {
    TranslateContext context = new TranslateContext(null);
    SqlASTNode sqlAST = buildAST(sql);
    try {
      fbt.transform(sqlAST, context);
      String fbs= context.getQInfoRoot().toFilterBlockStringTree();
      System.out.println(fbs);
      assertEquals(fbTree, sqlAST.toStringTree());
    } catch (SqlXlateException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  void printTreeType3(org.antlr33.runtime.tree.Tree tree) {

    System.out.println(this.toTypeString3(tree));
  }

  String toTypeString3(org.antlr33.runtime.tree.Tree tree) {

    StringBuilder sb = new StringBuilder();
    toTypeStringBuilder3(sb, tree);
    return sb.toString();
  }

  void toTypeStringBuilder3(StringBuilder sb, org.antlr33.runtime.tree.Tree tree) {

    sb.append(" ");
    if (tree.getChildCount() > 0) {
      sb.append("[");
    }

    sb.append(tree.getType());

    if (tree.getChildCount() > 0) {
      for (int i = 0; i < tree.getChildCount(); i++) {
        toTypeStringBuilder3(sb, tree.getChild(i));
      }
      sb.append("]");
    }

  }

  void printTreeType(org.antlr.runtime.tree.Tree tree) {

    System.out.println(this.toTypeString(tree));
  }

  String toTypeString(org.antlr.runtime.tree.Tree tree) {

    StringBuilder sb = new StringBuilder();
    toTypeStringBuilder(sb, tree);
    return sb.toString();
  }

  void toTypeStringBuilder(StringBuilder sb, org.antlr.runtime.tree.Tree tree) {

    sb.append(" ");
    if (tree.getChildCount() > 0) {
      sb.append("[");
    }

    sb.append(tree.getType());

    if (tree.getChildCount() > 0) {
      for (int i = 0; i < tree.getChildCount(); i++) {
        toTypeStringBuilder(sb, tree.getChild(i));
      }
      sb.append("]");
    }

  }


}
