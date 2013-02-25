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

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

public class ParserTest extends BaseSQLTest{


  public void testHive() throws Exception {
    String command = "explain select ngrams(sentences(lower(contents)), 5, 100, 1000).estfrequency from kafka";
    ParseDriver pd = new ParseDriver();
    Tree tree = pd.parse(command, null);
    System.out.println(tree.toStringTree());
    System.out.println(toTypeString(tree));
  }

  public void testPLSQL(){
    String command = "explain plan for select ngrams(sentences(lower(contents)), 5, 100, 1000).estfrequency from kafka";
    org.antlr33.runtime.tree.Tree tree =  super.buildAST(command);
    System.out.println(tree.toStringTree());
  }


  @Override
  String toTypeString(Tree tree) {

    StringBuilder sb = new StringBuilder();
    toTypeStringBuilder(sb, tree);
    return sb.toString();
  }

  @Override
  void toTypeStringBuilder(StringBuilder sb, Tree tree) {

    sb.append(" ");
    if (tree.getChildCount() > 0) {
      sb.append("[");
    }

    sb.append(tree.getType());

    if (tree.getChildCount() > 0) {
      for (int i = 0; i < tree.getChildCount(); i++) {
        toTypeStringBuilder(sb, tree.getChild(i));
      }
      sb.append("]");
    }

  }
}
