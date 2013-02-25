package org.apache.hadoop.hive.ql.parse.sql.generator;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.sql.SqlASTNode;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;

public class ErrorGenerator extends BaseHiveASTGenerator{


  @Override
  public boolean generate(ASTNode hiveRoot, SqlASTNode sqlRoot, ASTNode currentHiveNode,
      SqlASTNode currentSqlNode, TranslateContext context) throws Exception {
    // log error
    return false;
  }

}
