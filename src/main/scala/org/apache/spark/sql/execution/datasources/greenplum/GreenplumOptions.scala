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

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.util.Utils

/**
 * Options for the Greenplum data source.
 */
case class GreenplumOptions(@transient params: CaseInsensitiveMap[String])
  extends JDBCOptions(params.updated("driver", "org.postgresql.Driver")) {

  val delimiter: String = params.getOrElse("delimiter", ",")
  assert(delimiter.length == 1, "The delimiter should be a single character.")

  /**
   * This option is only used for these cases:
   * 1. overwrite a gptable, which is a CascadingTruncateTable.
   * 2. append data to a gptable.
   */
  val transactionOn: Boolean = params.getOrElse("transactionOn", "false").toBoolean

  /** Max number of times we are allowed to retry dropTempTable operation. */
  val dropTempTableMaxRetries: Int = 3

  /** Timeout for copying a partition's data to greenplum. */
  val copyTimeout = Utils.timeStringAsMs(params.getOrElse("copyTimeout", "1h"))
  assert(copyTimeout > 0, "The copy timeout should be positive, 10s, 10min, 1h etc.")

  /** Max task numbers write Greenplum concurrently */
  val maxConnections = params.getOrElse("maxConnections", "20").toInt
}
