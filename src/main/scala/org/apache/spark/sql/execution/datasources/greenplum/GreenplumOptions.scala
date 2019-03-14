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

import java.util.{Locale, TimeZone}

import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

/**
 * Options for the Greenplum data source.
 */
case class GreenplumOptions(
    @transient params: CaseInsensitiveMap[String],
    defaultTimeZoneId: String)
  extends JDBCOptions(params.updated("driver", "org.postgresql.Driver")) {

  val delimiter: String = params.getOrElse("delimiter", ",")
  val timeZone: TimeZone = DateTimeUtils.getTimeZone(
    params.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

  // Uses `FastDateFormat` which can be direct replacement for `SimpleDateFormat` and thread-safe.
  val dateFormat: FastDateFormat =
    FastDateFormat.getInstance(params.getOrElse("dateFormat", "yyyy-MM-dd"), Locale.US)

  val timestampFormat: FastDateFormat =
    FastDateFormat.getInstance(
      params.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), timeZone, Locale.US)
}
