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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.sql.SqlXlateException;
import org.apache.hadoop.hive.ql.parse.sql.transformer.fb.FilterBlockUtil;

import br.com.porcelli.parser.plsql.PantheraParser_PLSQLParser;

/**
 * build FilterBlockProcessor by subquery node. <br>
 * FilterBlockProcessorFactory.
 *
 */
public class FilterBlockProcessorFactory {

  private static final Log LOG = LogFactory.getLog(FilterBlockProcessorFactory.class);

  public static FilterBlockProcessor getUnCorrelatedProcessor(CommonTree subQ)
      throws SqlXlateException {
    int type = subQ.getType();
    switch (type) {
    case PantheraParser_PLSQLParser.EQUALS_OP:
    case PantheraParser_PLSQLParser.NOT_EQUAL_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP:
      if (rebuildCompareSubquery(subQ)) {
        return getUnCorrelatedProcessor(subQ);
      } else {
        return new CompareProcessor4UC();
      }
    case PantheraParser_PLSQLParser.NOT_IN:
      return new NotInProcessor4UC();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_IN:
      return new InProcessor4UC();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_EXISTS:
      return new ExistsProcessor4UC();
    default:
      throw new SqlXlateException("Unimplement uncorrelated subquery type:" + type);
    }

  }

  public static FilterBlockProcessor getCorrelatedProcessor(CommonTree subQ)
      throws SqlXlateException {
    int type = subQ.getType();
    switch (type) {
    case PantheraParser_PLSQLParser.EQUALS_OP:
    case PantheraParser_PLSQLParser.NOT_EQUAL_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP:
      if (rebuildCompareSubquery(subQ)) {
        return getCorrelatedProcessor(subQ);
      } else {
        return new CompareProcessor4C();
      }
    case PantheraParser_PLSQLParser.SQL92_RESERVED_EXISTS:
      return new ExistsProcessor4C();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_IN:
      return new InProcessor4C();
    case PantheraParser_PLSQLParser.NOT_IN:
      return new NotInProcessor4C();
    default:
      throw new SqlXlateException("Unimplement correlated subquery type:" + type);
    }
  }

  public static FilterBlockProcessor getHavingUnCorrelatedProcessor(CommonTree subQ)
      throws SqlXlateException {
    int type = subQ.getType();
    switch (type) {
    case PantheraParser_PLSQLParser.EQUALS_OP:
    case PantheraParser_PLSQLParser.NOT_EQUAL_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP:
      if (rebuildCompareSubquery(subQ)) {
        return getHavingUnCorrelatedProcessor(subQ);
      } else {
        return new CompareProcessor4HavingUC();
      }
    case PantheraParser_PLSQLParser.NOT_IN:
      return new NotInProcessor4HavingUC();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_IN:
      return new InProcessor4HavingUC();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_EXISTS:
      return new ExistsProcessor4HavingUC();
    default:
      throw new SqlXlateException("Unimplement uncorrelated having subquery type:" + type);
    }
  }

  public static FilterBlockProcessor getHavingCorrelatedProcessor(CommonTree subQ)
      throws SqlXlateException {
    int type = subQ.getType();
    switch (type) {
    case PantheraParser_PLSQLParser.EQUALS_OP:
    case PantheraParser_PLSQLParser.NOT_EQUAL_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OP:
    case PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP:
    case PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP:
      if (rebuildCompareSubquery(subQ)) {
        return getHavingCorrelatedProcessor(subQ);
      } else {
        return new CompareProcessor4HavingC();
      }
    case PantheraParser_PLSQLParser.NOT_IN:
      return new NotInProcessor4HavingC();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_IN:
      return new InProcessor4HavingC();
    case PantheraParser_PLSQLParser.SQL92_RESERVED_EXISTS:
      return new ExistsProcessor4HavingC();
    default:
      throw new SqlXlateException("Unimplement correlated having subquery type:" + type);
    }
  }

  public static FilterBlockProcessor getSimpleProcessor() {
    return new SimpleFilterBlokcProcessor();
  }

  private FilterBlockProcessorFactory() {
  }

  /**
   * rebuild sub query by removing scope node(ALL, SOME, ANY)
   *
   * @param subQ
   * @return
   */
  private static boolean rebuildCompareSubquery(CommonTree subQ) {
    int scopeType = subQ.getChild(1).getType();
    int compareType = subQ.getType();
    boolean isRebuild = true;
    if (scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ALL
        && (compareType == PantheraParser_PLSQLParser.GREATER_THAN_OP || compareType == PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP)) {// >all
      rebuildScopeType(subQ, "max");
    } else if (scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ALL
        && (compareType == PantheraParser_PLSQLParser.LESS_THAN_OP || compareType == PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP)) {
      rebuildScopeType(subQ, "min");
    } else if (scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ALL
        && (compareType == PantheraParser_PLSQLParser.NOT_EQUAL_OP)) {
      rebuildCompareType(subQ, PantheraParser_PLSQLParser.NOT_IN, "NOT_IN");
    } else if (scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ALL
        && (compareType == PantheraParser_PLSQLParser.EQUALS_OP)) {
      // FIXME create UDAF which return only one column or NULL
      deleteScopeNode(subQ);
    } else if ((scopeType == PantheraParser_PLSQLParser.SOME_VK || scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ANY)
        && (compareType == PantheraParser_PLSQLParser.GREATER_THAN_OP || compareType == PantheraParser_PLSQLParser.GREATER_THAN_OR_EQUALS_OP)) {
      rebuildScopeType(subQ, "min");
    } else if ((scopeType == PantheraParser_PLSQLParser.SOME_VK || scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ANY)
        && (compareType == PantheraParser_PLSQLParser.LESS_THAN_OP || compareType == PantheraParser_PLSQLParser.LESS_THAN_OR_EQUALS_OP)) {
      rebuildScopeType(subQ, "max");
    } else if ((scopeType == PantheraParser_PLSQLParser.SOME_VK || scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ANY)
        && (compareType == PantheraParser_PLSQLParser.EQUALS_OP)) {
      rebuildCompareType(subQ, PantheraParser_PLSQLParser.SQL92_RESERVED_IN, "in");
    } else if ((scopeType == PantheraParser_PLSQLParser.SOME_VK || scopeType == PantheraParser_PLSQLParser.SQL92_RESERVED_ANY)
        && (compareType == PantheraParser_PLSQLParser.NOT_EQUAL_OP)) {
      // TODO UDAF?
      deleteScopeNode(subQ);
    } else {
      isRebuild = false;
    }
    LOG.info("After preprocess, subquery ast is:"
        + subQ.toStringTree().replace('(', '[').replace(')', ']'));
    return isRebuild;
  }

  private static void rebuildScopeType(CommonTree subQ, String aggregationName) {
    // add aggregation function
    CommonTree subQuery = (CommonTree) subQ.getChild(1).getChild(0);
    CommonTree select = (CommonTree) subQuery.getChild(0);
    CommonTree selectItem = (CommonTree) select.getFirstChildWithType(
        PantheraParser_PLSQLParser.SELECT_LIST).getChild(0);
    CommonTree expr = (CommonTree) selectItem.getChild(0);
    if (expr.getChild(0).getType() != PantheraParser_PLSQLParser.STANDARD_FUNCTION) {
      CommonTree function = FilterBlockUtil.createFunction(aggregationName, (CommonTree) expr
          .deleteChild(0));
      FilterBlockUtil.attachChild(expr, function);
    }
    // deletes scope node
    subQ.deleteChild(1);
    subQ.addChild(subQuery);
  }

  private static void deleteScopeNode(CommonTree subQ) {
    CommonTree subQuery = (CommonTree) subQ.getChild(1).getChild(0);
    subQ.deleteChild(1);
    subQ.addChild(subQuery);
  }

  private static void rebuildCompareType(CommonTree subQ, int type, String text) {
    subQ.getToken().setType(type);
    subQ.getToken().setText(text);
  }
}
