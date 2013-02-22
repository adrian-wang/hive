package org.apache.hadoop.hive.ql.parse.sql.transformer.fb.processor;

import org.apache.hadoop.hive.ql.parse.sql.PantheraExpParser;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

public class AndProcessor extends CommonFilterBlockProcessor {

  /**
   * template method
   */
  @Override
  public void process(FilterBlockContext fbContext, FilterBlock fb, TranslateContext context)
      throws SqlXlateException {
    topQuery = fbContext.getQueryStack().firstElement();//TODO how about group?
    topSelect = fb.getChildren().get(0).getTransformedNode();
    bottomSelect = fb.getChildren().get(1).getTransformedNode();
    this.fbContext = fbContext;
    this.fb = fb;
    this.context = context;
    processFB();
    fb.setTransformedNode(super.topSelect);
  }

  @Override
  void processFB() throws SqlXlateException {
    super.processAnd(FilterBlockUtil.createSqlASTNode(PantheraExpParser.LEFTSEMI_VK, "leftsemi"));

  }
}
