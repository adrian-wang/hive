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
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlock;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockContext;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 *
 * SimpleFilterBlokcProcessor.
 *
 */
public class SimpleFilterBlokcProcessor extends BaseFilterBlockProcessor {


  @Override
  public void process(FilterBlockContext fbContext, FilterBlock fb, TranslateContext context) {
    super.context = context;
    super.fbContext = fbContext;
    topSelect = fbContext.getQueryStack().peek().cloneSimpleQuery();
    super.buildSelectListAlias(null, (CommonTree) topSelect
        .getFirstChildWithType(PantheraParser_PLSQLParser.SELECT_LIST));

    super.builldSimpleWhere(fb.getASTNode());

    fb.setTransformedNode(topSelect);
  }

  @Override
  void processFB() throws SqlXlateException {
    // TODO Auto-generated method stub

  }

}
