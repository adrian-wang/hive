package org.apache.hadoop.hive.ql.parse.sql.generator;

import org.antlr33.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

public class ErrorGenerator extends BaseHiveASTGenerator{


  @Override
  public boolean generate(ASTNode hiveRoot, CommonTree sqlRoot, ASTNode currentHiveNode,
      CommonTree currentSqlNode, TranslateContext context) throws Exception {
    // log error
    return false;
  }

}
