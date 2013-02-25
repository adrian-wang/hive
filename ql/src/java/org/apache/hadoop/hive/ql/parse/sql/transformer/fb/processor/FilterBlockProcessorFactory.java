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

import java.util.Map;

import org.apache.hadoop.hive.ql.parse.sql.PantheraMap;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

public class FilterBlockProcessorFactory {

  private static Map<Integer, FilterBlockProcessor> unCorrelatedTransferMap = new PantheraMap<FilterBlockProcessor>();
  private static Map<Integer, FilterBlockProcessor> correlatedTransferMap = new PantheraMap<FilterBlockProcessor>();


  static {
    unCorrelatedTransferMap.put(PantheraParser_PLSQLParser.GREATER_THAN_OP, new GreaterThanProcessor4UC());
  }

  static {

  }

  public static FilterBlockProcessor getUnCorrelatedTransfer(int type) {
    return unCorrelatedTransferMap.get(type);
  }

  public static FilterBlockProcessor getCorrelatedTransfer(int type) {
    return correlatedTransferMap.get(type);
  }

  public static FilterBlockProcessor getSimpleTransfer() {
    return new SimpleFilterBlokcProcessor();
  }

  private FilterBlockProcessorFactory() {
  }
}
