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

import java.util.{Date, TimeZone}

import org.apache.commons.lang3.time.FastDateFormat
import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}

class GreenplumOptionsSuite extends SparkFunSuite with Matchers {
  private val date = new Date(0)
  val timeZoneId: String = TimeZone.getDefault.getID

  test("empty user specified options") {
    val e = intercept[IllegalArgumentException](GreenplumOptions(CaseInsensitiveMap(
      Map()), timeZoneId))
    e.getMessage should include("Option 'url' is required")
  }

  test("map with only url") {
    val e = intercept[IllegalArgumentException](GreenplumOptions(CaseInsensitiveMap(
      Map("url" -> "")), timeZoneId))
    e.getMessage should include("Option 'dbtable' is required")
  }

  test("driver class should always using postgresql") {
    val options = GreenplumOptions(CaseInsensitiveMap(
      Map("url" -> "", "dbtable" -> "src")), timeZoneId)
    options.driverClass should be("org.postgresql.Driver")
    val options2 = GreenplumOptions(CaseInsensitiveMap(
      Map("url" -> "",
        "dbtable" -> "src",
        "driver" -> "org.mysql.Driver")), timeZoneId)
    options2.driverClass should be("org.postgresql.Driver")
  }

  test("time zone") {
    val options = GreenplumOptions(CaseInsensitiveMap(
      Map("url" -> "", "dbtable" -> "src")), timeZoneId)
    options.timeZone should be(DateTimeUtils.getTimeZone(options.defaultTimeZoneId))
    val tid = TimeZone.getTimeZone("PST").getID
    val options2 = GreenplumOptions(CaseInsensitiveMap(
      Map("url" -> "", "dbtable" -> "src", "timeZone" -> tid)), timeZoneId)
    options2.timeZone should be(DateTimeUtils.getTimeZone(tid))
  }

  test("date format") {
    val options = GreenplumOptions(CaseInsensitiveMap(
      Map("url" -> "", "dbtable" -> "src")), timeZoneId)
    options.dateFormat.format(date) should be("1970-01-01")
    val options2 = GreenplumOptions(CaseInsensitiveMap(
      Map("url" -> "", "dbtable" -> "src", "dateFormat" -> "MM/dd/yyyy" )), timeZoneId)
    options2.dateFormat.format(date) should be("01/01/1970")
  }

  test("time format") {
    val options = GreenplumOptions(CaseInsensitiveMap(
      Map("url" -> "", "dbtable" -> "src")), timeZoneId)
    val time1 = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(date)
    options.timestampFormat.format(date) should be(time1)
    val time2 = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss").format(date)
    val options2 = GreenplumOptions(CaseInsensitiveMap(
      Map("url" -> "", "dbtable" -> "src",
        "timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss" )), timeZoneId)
    options2.timestampFormat.format(date) should be(time2)
  }

  test("as properties") {
    val options = GreenplumOptions(CaseInsensitiveMap(
      Map("url" -> "", "dbtable" -> "src")), timeZoneId)
    val properties = options.asProperties
    properties.getProperty("url") should be("")
    properties.get("dbtable") should be("src")
    properties.get("driver") should be("org.postgresql.Driver")
  }

  test("as connection properties") {
    val options = GreenplumOptions(CaseInsensitiveMap(
      Map("url" -> "", "dbtable" -> "src")), timeZoneId)
    val properties = options.asConnectionProperties
    properties.getProperty("url") should be(null)
    properties.get("dbtable") should be(null)
    properties.get("driver") should be(null)
  }
}
