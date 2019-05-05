/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.greenplum

import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}

class DefaultSource
  extends RelationProvider with CreatableRelationProvider with DataSourceRegister {

  import GreenplumUtils._
  import JdbcUtils._

  override def shortName(): String = "greenplum"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    import JDBCOptions._

    val options =
      GreenplumOptions(CaseInsensitiveMap(parameters), sqlContext.conf.sessionLocalTimeZone)
    val partitionColumn = options.partitionColumn
    val lowerBound = options.lowerBound
    val upperBound = options.upperBound
    val numPartitions = options.numPartitions

    val partitionInfo = if (partitionColumn.isEmpty) {
      assert(lowerBound.isEmpty && upperBound.isEmpty, "When 'partitionColumn' is not specified, " +
        s"'$JDBC_LOWER_BOUND' and '$JDBC_UPPER_BOUND' are expected to be empty")
      null
    } else {
      assert(lowerBound.nonEmpty && upperBound.nonEmpty && numPartitions.nonEmpty,
        s"When 'partitionColumn' is specified, '$JDBC_LOWER_BOUND', '$JDBC_UPPER_BOUND', and " +
          s"'$JDBC_NUM_PARTITIONS' are also required")
      JDBCPartitioningInfo(
        partitionColumn.get, lowerBound.get, upperBound.get, numPartitions.get)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)
    GreenplumRelation(parts, options)(sqlContext.sparkSession)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame): BaseRelation = {
    val options =
      GreenplumOptions(CaseInsensitiveMap(parameters), sqlContext.conf.sessionLocalTimeZone)
    val conn = createConnectionFactory(options)()
    try {
      if (tableExists(conn, options)) {
        mode match {
          case SaveMode.Overwrite
            if options.isTruncate && isCascadingTruncateTable(options.url).contains(false) =>
            truncateTable(conn, options)
            val tableSchema = getSchemaOption(conn, options).getOrElse(df.schema)
            copyToGreenplum(df, tableSchema, options)
          case SaveMode.Overwrite =>
            dropTable(conn, options.table)
            createTable(conn, df, options)
            copyToGreenplum(df, df.schema, options)
          case SaveMode.Append =>
            val tableSchema = getSchemaOption(conn, options).getOrElse(df.schema)
            copyToGreenplum(df, tableSchema, options)
          case SaveMode.ErrorIfExists =>
            throw new AnalysisException(s"Table or view '${options.table}' already exists. $mode")
          case SaveMode.Ignore => // do nothing
        }
      } else {
        createTable(conn, df, options)
        copyToGreenplum(df, df.schema, options)
      }
    } finally {
      conn.close()
    }
    createRelation(sqlContext, parameters)
  }
}
