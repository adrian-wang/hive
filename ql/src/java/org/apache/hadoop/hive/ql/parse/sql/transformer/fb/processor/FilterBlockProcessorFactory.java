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

import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 *
 * FilterBlockProcessorFactory.
 *
 */
public class FilterBlockProcessorFactory {



  public static FilterBlockProcessor getUnCorrelatedProcessor(int type) throws SqlXlateException {
    switch (type) {
    case PantheraParser_PLSQLParser.GREATER_THAN_OP:
    case PantheraParser_PLSQLParser.EQUALS_OP:
      return new CompareProcessor4UC();
    case PantheraParser_PLSQLParser.NOT_IN:
      return new NotInProcessor4UC();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_IN:
      return new InProcessor4UC();
    default:
      throw new SqlXlateException("Unimplement sub query type:" + type);
    }

  }

  public static FilterBlockProcessor getCorrelatedProcessor(int type) throws SqlXlateException {
    switch (type) {
    case PantheraParser_PLSQLParser.EQUALS_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP:
      return new CompareOpProcessor4C();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_EXISTS:
      return new ExistsProcessor4C();
    default:
      throw new SqlXlateException("Unimplement sub query type:" + type);
    }
  }

  public static FilterBlockProcessor getHavingUnCorrelatedProcessor(int type)
      throws SqlXlateException {
    switch (type) {
    case PantheraParser_PLSQLParser.GREATER_THAN_OP:
      return new GreaterThanProcessor4HavingUC();
    default:
      throw new SqlXlateException("Unimplement sub query type:" + type);
    }
  }

  public static FilterBlockProcessor getSimpleProcessor() {
    return new SimpleFilterBlokcProcessor();
  }

  private FilterBlockProcessorFactory() {
  }

  public static FilterBlockProcessor getAndProcessor() {
    return new AndProcessor();
  }
}
