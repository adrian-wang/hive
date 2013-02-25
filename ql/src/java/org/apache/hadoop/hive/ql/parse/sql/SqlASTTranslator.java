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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.sql.generator.GeneratorFactory;
import org.apache.hadoop.hive.ql.parse.sql.generator.HiveASTGenerator;
import org.apache.hadoop.hive.ql.parse.sql.transformer.SqlASTTransformer;
import org.apache.hadoop.hive.ql.parse.sql.transformer.TransformerBuilder;


/**
 * The class to translate SQL AST to Hive AST.
 * translate() is the main processing method.
 * translate() now calls HiveASTGenerator to generate Hive AST from SQL AST.
 * later we may add more validations and optimizations after AST generation later.
 */
public class SqlASTTranslator {

  private static final Log LOG = LogFactory.getLog("hive.ql.parse.sql.SqlASTTranslator");

  private SqlASTTransformer transformer;
  private final HiveConf conf;

  public SqlASTTranslator(HiveConf conf) {
    this.conf = conf;
  }

  /**
   * Translate SQL AST to Hive AST.
   *
   * @param SqlASTRoot
   *          The root node of input SQL AST
   * @return The root node of generated Hive AST
   * @throws HiveException
   */
  public ASTNode translate(SqlASTNode sqlASTRoot) throws SqlXlateException {
    ASTNode ret = null;
    LOG.info("Starting Translation from SQL AST to Hive AST");
    LOG.info("Original SQL AST : " + sqlASTRoot.toStringTree());

    TranslateContext context = new TranslateContext();
    context.setConf(conf);

    if (transformer == null) {
      transformer = TransformerBuilder.buildTransformer();
    }
    transformer.transform(sqlASTRoot, context);
    LOG.info("Transformed SQL AST : " + sqlASTRoot.toStringTree());

    HiveASTGenerator generator = GeneratorFactory.getGenerator(sqlASTRoot);
    ASTNode hiveTopNode = new ASTNode();
    try {
      // long b = System.currentTimeMillis();
      // for (int i = 0; i < 1000; i++) {
      generator.generateHiveAST(hiveTopNode, sqlASTRoot, hiveTopNode, sqlASTRoot, context);
      // }
      // long e = System.currentTimeMillis();
      // System.out.println("--------"+(e-b));
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new SqlXlateException(e);
    }

    ret = (ASTNode) hiveTopNode.getChild(0);

    // potential validation, optimization here
    LOG.info("Generated Hive AST : " + ret.toStringTree());
    return ret;
  }

  public void setTransformer(SqlASTTransformer transformer) {
    this.transformer = transformer;
  }


}
