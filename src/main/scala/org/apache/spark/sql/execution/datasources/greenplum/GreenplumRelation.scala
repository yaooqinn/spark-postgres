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

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCRDD, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types._

private[sql] case class GreenplumRelation(
    parts: Array[Partition], options: GreenplumOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {

  import JdbcUtils._
  import GreenplumUtils._

  override def sqlContext: SQLContext = sparkSession.sqlContext
  override val needConversion: Boolean = false
  override val schema: StructType = {
    val tableSchema = JDBCRDD.resolveTable(options)
    options.customSchema match {
      case Some(customSchema) => JdbcUtils.getCustomSchema(
        tableSchema, customSchema, sparkSession.sessionState.conf.resolver)
      case None => tableSchema
    }
  }

  // Check if JDBCRDD.compileFilter can accept input filters
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter(JDBCRDD.compileFilter(_, JdbcDialects.get(options.url)).isEmpty)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    JDBCRDD.scanTable(
      sparkSession.sparkContext,
      schema,
      requiredColumns,
      filters,
      parts,
      options).asInstanceOf[RDD[Row]]
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val conn = createConnectionFactory(options)()
    if (tableExists(conn, options)) {
      if (overwrite) {
        if (options.isTruncate && isCascadingTruncateTable(options.url).contains(false)) {
          truncateTable(conn, options)
          copyToGreenplum(data, schema, options)
        } else {
          dropTable(conn, options.table)
          createTable(conn, data, options)
          copyToGreenplum(data, schema, options)
        }
      } else {
        copyToGreenplum(data, schema, options)
      }
    } else {
      createTable(conn, data, options)
      copyToGreenplum(data, schema, options)
    }
    data.toLocalIterator()
  }

  override def toString: String = {
    val partitioningInfo = if (parts.nonEmpty) s" [numPartitions=${parts.length}]" else ""
    // credentials should not be included in the plan output, table information is sufficient.
    s"GreenplumRelation(${options.table})" + partitioningInfo
  }
}
