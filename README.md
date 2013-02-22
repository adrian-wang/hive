# An Analytical SQL Engine for MapReduce #

####Efforts Required for Full SQL-92 Support####

Higher level languages or APIs (such as Pig, Hive and Cascading) significantly lower the barrier to MapReduce based data analytics. On the other hand, SQL is still the most important query language in modern business application environment, with more than 30 years of investments by the industry. There is a wealth of business users, enterprise analytics applications and third-party tools (such as query builders and BI applications) that all require full SQL support.

While there are continuous efforts in extending Hive’s SQL support (e.g., see some recent examples such as [HIVE-2005](https://issues.apache.org/jira/browse/HIVE-2005) and [HIVE-2810](https://issues.apache.org/jira/browse/HIVE-2810)), many widely used SQL constructs are still not supported in Hive, such as selecting from multiple tables as follows:

`select * from t1,t2 where t1.c > t2.z;`

#### Our Approach to an Analytical SQL Engine for MapReduce ####

We have taken a different approach to build a SQL-92 compatible engine for MapReduce based analytical query processing. It uses an open source SQL parser (<https://github.com/porcelli/plsql-parser>) to generate AST (abstract syntax tree) for the SQL query, then transforms the SQL AST to MapReduce jobs through a series of context-aware analyses and optimizations (reusing the semantic analyzer and optimizer in Hive when appropriate), as illustrated in the figure below.

<img src="http://cloud.github.com/downloads/intel-hadoop/hive-0.9-panthera/sql_engine.jpg" alt="SQL Engine" width="231" height="276" />

#### Important SQL constructs Enabled  by the SQL Engine####

We have made our SQL engine implementation available as an extension to Hive. Currently it provides support for many common SQL constructs used in analytic queries by our users and customers, including some important features that are not directly supported in Hive today, such as:

* Subquery in WHERE clauses (using ALL, ANY, IN, EXIST, SOME keywords); e.g.,

  `select * from t1 where t1.d > ALL (select z from t2 where t2.z!=9);`

* Correlated subquery (i.e., a subquery that refers to a column of a table that is not in its FROM clause); e.g.,

  `select * from t1 where exists ( select * from t2 where t1.b = t2.y );`

* Scalar subquery (i.e., a subquery that returns exactly one column value from one row), whose output can be treated as a column or even used an expression within a SELECT statement; e.g.,

  `select a,b,c,d,e,(select z from t2 where t2.y = t1.b and z != 99 ) from t1;`

* Top-level subquery; e.g.,

  `select * from t1 union all select * from t2 union all select * from t3 order by 1,2;`

* Multiple-table SELECT statement; e.g., 

  `select * from t1,t2 where t1.c > t2.z;`


####Summary####
We will gradually contribute the implementation of the SQL-92 compatible engine to the Apache Hadoop community under Project Panthera (<https://github.com/intel-hadoop/project-panthera>). Please also refer to [Hive-3472](https://issues.apache.org/jira/browse/HIVE-3472) that tracks our efforts to collaborate with the Hadoop community to get the idea reviewed and hopefully incorporated into Apache Hive.
