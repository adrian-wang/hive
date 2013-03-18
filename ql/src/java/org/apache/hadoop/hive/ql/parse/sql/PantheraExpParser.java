package org.apache.hadoop.hive.ql.parse.sql;

import org.antlr33.runtime.TokenStream;

import br.com.porcelli.parser.plsql.PantheraParser;
import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class PantheraExpParser extends PantheraParser_PLSQLParser {

  public static final int PANTHERA_LIMIT = 999;
  public static final int LEFTSEMI_VK = 998;
  public static final int LIMIT_VK = 997;

  public static final String LEFTSEMI_STR = "leftsemi";
  public static final String LEFT_STR="left";

  public PantheraExpParser(TokenStream input, PantheraParser gPantheraParser) {
    super(input, gPantheraParser);
  }

}
