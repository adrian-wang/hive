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
package org.apache.hadoop.hive.ql.parse.sql.transformer.fb;

import java.util.EmptyStackException;

import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.TranslateContext;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.processor.FilterBlockProcessorFactory;


public abstract class NormalFilterBlack extends BaseFilterBlock {

  /**
   * TODO
   *
   * @param fbContext
   * @param context
   * @param subqFB
   * @throws SqlXlateException
   */
  void processStackSubq(FilterBlockContext fbContext, TranslateContext context)
      throws SqlXlateException {
    SubQFilterBlock subqFB;
    QueryBlock queryBlock;
    SubQFilterBlock currentSubqFB;
    try {
      subqFB = fbContext.getSubQStack().pop();
    } catch (EmptyStackException e) {
      return;
    }
    try {
      queryBlock = fbContext.getQueryStack().pop();
    } catch (EmptyStackException e) {
      fbContext.getSubQStack().push(subqFB);
      return;
    }
    try {
      currentSubqFB = fbContext.getSubQStack().peek();
    } catch (EmptyStackException e) {
      fbContext.getSubQStack().push(subqFB);
      fbContext.getQueryStack().push(queryBlock);
      return;
    }
    // multi level sub query's non-bottom-query must be uncorrelated.
    FilterBlockProcessorFactory.getUnCorrelatedProcessor(
        currentSubqFB.getASTNode().getType()).process(fbContext, this, context);
    this.processStackSubq(fbContext, context);
    fbContext.getSubQStack().push(subqFB);
    fbContext.getQueryStack().push(queryBlock);
  }
}
