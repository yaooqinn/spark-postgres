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

import java.io.File
import java.sql.{Connection, SQLException}
import java.util.TimeZone

import io.airlift.testing.postgresql.TestingPostgreSqlServer
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class GreenplumUtilsSuite extends SparkFunSuite with MockitoSugar {
  val timeZoneId: String = TimeZone.getDefault.getID

  var postgres: TestingPostgreSqlServer = _
  var url: String = _
  var sparkSession: SparkSession = _
  var tempDir: File = _

  override def beforeAll(): Unit = {
    tempDir = Utils.createTempDir()
    postgres = new TestingPostgreSqlServer("gptest", "gptest")
    url = postgres.getJdbcUrl
    sparkSession = SparkSession.builder()
      .config("spark.master", "local")
      .config("spark.app.name", "testGp")
      .config("spark.sql.warehouse.dir", s"${tempDir.getAbsolutePath}/warehouse")
      .config("spark.local.dir", s"${tempDir.getAbsolutePath}/local")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (postgres != null) {
        postgres.close()
      }
      if (sparkSession != null) {
        sparkSession.stop()
      }
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  ignore("make converter") {
    val options = GreenplumOptions(CaseInsensitiveMap(
      Map("url" -> "", "dbtable" -> "src")), timeZoneId)

    val row1 = Row(true, 1.toByte, 2.toShort, 3, 4.toLong,
      5.toFloat, 6.toDouble, 7.toString, 8.toString.getBytes,
      9,
      10L,
      new java.math.BigDecimal(11),
      Array[String]("12", "12"),
      Map(13 -> 13, 130 -> 130),
      Row(14, "15"))

    val row2 = Row(null)

    val boolConverter = GreenplumUtils.makeConverter(BooleanType, options)
    assert(boolConverter.apply(row1, 0) === "true")
    intercept[NullPointerException](boolConverter.apply(row2, 0) === "")

    val byteConverter = GreenplumUtils.makeConverter(ByteType, options)
    assert(byteConverter(row1, 1) === "1")

    val shortConverter = GreenplumUtils.makeConverter(ShortType, options)
    assert(shortConverter(row1, 2) === "2")

    val intConverter = GreenplumUtils.makeConverter(IntegerType, options)
    assert(intConverter(row1, 3) === "3")

    val longConverter = GreenplumUtils.makeConverter(LongType, options)
    assert(longConverter(row1, 4) === "4")

    val floatConverter = GreenplumUtils.makeConverter(FloatType, options)
    assert(floatConverter(row1, 5) === "5.0")

    val doubleConverter = GreenplumUtils.makeConverter(DoubleType, options)
    assert(doubleConverter(row1, 6) === "6.0")

    val strConverter = GreenplumUtils.makeConverter(StringType, options)
    assert(strConverter(row1, 7) === "7")

    val binConverter = GreenplumUtils.makeConverter(BinaryType, options)
    assert(binConverter(row1, 8) === "8")

    val dateConverter = GreenplumUtils.makeConverter(DateType, options)
    assert(dateConverter(row1, 9) === options.dateFormat.format(DateTimeUtils.toJavaDate(9)))

    val tsConverter = GreenplumUtils.makeConverter(TimestampType, options)
    assert(tsConverter(row1, 10) ===
      options.timestampFormat.format(DateTimeUtils.toJavaTimestamp(10)))

    val decimalConverter = GreenplumUtils.makeConverter(DecimalType(2, 0), options)
    assert(decimalConverter(row1, 11) === new java.math.BigDecimal(11).toString)

    val arrConverter = GreenplumUtils.makeConverter(ArrayType(StringType), options)
    assert(arrConverter(row1, 12) === Array[String]("12", "12").mkString("[", ",", "]"))

    val mapConverter = GreenplumUtils.makeConverter(MapType(IntegerType, IntegerType), options)
    assert(mapConverter(row1, 13) ===
      Map(13 -> 13, 130 ->130)
        .map(e => e._1 + ":" + e._2).toSeq.sorted.mkString("{", ",", "}"))

    val structConverter =
      GreenplumUtils.makeConverter(
        StructType(Array(StructField("a", IntegerType), StructField("b", StringType))), options)
    assert(structConverter(row1, 14) === "{\"a\":14,\"b\":15}")
  }

  test("test copy to greenplum") {
    withConnectionAndOptions { (conn, tblname, options) =>
      // scalastyle:off
      val kvs = Map[Int, String](0 -> " ", 1 -> "\t", 2 -> "\n", 3 -> "\r", 4 -> "\\t",
        5 -> "\\n", 6 -> "\\", 7 -> ",", 8 -> "te\tst", 9 -> "1`'`", 10 -> "中文测试")
      // scalastyle:on
      val rdd = sparkSession.sparkContext.parallelize(kvs.toSeq)
      val df = sparkSession.createDataFrame(rdd)
      val stat1 = conn.createStatement()
      val strSchema = JdbcUtils.schemaString(df, options.url, options.createTableColumnTypes)
      stat1.execute(s"create table $tblname($strSchema)")

      GreenplumUtils.copyOverwriteToGreenplum(df, df.schema, options)
      val stat2 = conn.createStatement()
      stat2.executeQuery(s"select * from $tblname")
      stat2.setFetchSize(kvs.size + 1)
      var count = 0
      val result2 = stat2.getResultSet
      while (result2.next()) {
        val k = result2.getInt(1)
        val v = result2.getString(2)
        count += 1
        assert(kvs.get(k).get === v)
      }
      assert(count === kvs.size)

      // Append the df's data to gptbl, so the size will double.
      GreenplumUtils.copyAppendToGreenplum(df, df.schema, options)
      val stat3 = conn.createStatement()
      stat3.executeQuery(s"select * from $tblname")
      stat3.setFetchSize(kvs.size * 2 + 1)
      val result3 = stat3.getResultSet
      count = 0
      while (result3.next()) {
        count += 1
      }
      assert(count === kvs.size * 2)

      // Overwrite gptbl with df's data.
      GreenplumUtils.copyOverwriteToGreenplum(df, df.schema, options)
      val stat4 = conn.createStatement()
      stat4.executeQuery(s"select * from $tblname")
      stat4.setFetchSize(kvs.size + 1)
      val result4 = stat4.getResultSet
      count = 0
      while (result4.next()) {
        count += 1
      }
      assert(count === kvs.size)
    }
  }

  test("test covert value and row") {
    withConnectionAndOptions { (_, _, options) =>
      val value = "test\t\rtest\n\\n\\,"
      assert(GreenplumUtils.convertValue(value, '\t') === "test\\\t\\rtest\\n\\\\n\\\\,")

      val values = Array[Any]("\n", "\t", ",", "\r", "\\", "\\n")
      val schema = new StructType().add("c1", StringType).add("c2", StringType)
        .add("c3", StringType).add("c4", StringType).add("c5", StringType)
        .add("c6", StringType)
      val valueConverters: Array[(Row, Int) => String] =
        schema.map(s => GreenplumUtils.makeConverter(s.dataType, options)).toArray

      val row = new GenericRow(values)
      val str = GreenplumUtils.convertRow(row, schema, options, valueConverters)
      assert(str === "\\n\t\\\t\t,\t\\r\t\\\\\t\\\\n\n".getBytes("utf-8"))
    }
  }

  test("test copy partition") {
    withConnectionAndOptions { (conn, tblname, options) =>

      val values = Array[Any]("\n", "\t", ",", "\r", "\\", "\\n")
      val schema = new StructType().add("c1", StringType).add("c2", StringType)
        .add("c3", StringType).add("c4", StringType).add("c5", StringType)
        .add("c6", StringType)
      val rows = Array(new GenericRow(values)).toIterator

      val createTbl = s"CREATE TABLE $tblname(c1 text, c2 text, c3 text, c4 text, c5 text, c6 text)"
      GreenplumUtils.executeStatement(conn, createTbl)

      val accmulator = sparkSession.sparkContext.longAccumulator("test")

      GreenplumUtils.copyParition(rows, options, schema, tblname, accmulator)
      val stat = conn.createStatement()
      val sql = s"SELECT * FROM $tblname"
      stat.executeQuery(sql)
      val result = stat.getResultSet
      result.next()
      for (i <- (0 until values.size)) {
        assert(result.getObject(i + 1) === values(i))
      }
      assert(!result.next())
    }
  }

  test("test transactions support") {
    withConnectionAndOptions { (conn, tblname, options) =>
      // scalastyle:off
      val kvs = Map[Int, String](0 -> " ", 1 -> "\t", 2 -> "\n", 3 -> "\r", 4 -> "\\t",
        5 -> "\\n", 6 -> "\\", 7 -> ",", 8 -> "te\tst", 9 -> "1`'`", 10 -> "中文测试")
      // scalastyle:on
      val tempPrefix = "sparkGpTmp"
      val df = mock[DataFrame]
      val rdd = sparkSession.sparkContext.parallelize(kvs.toSeq)
      val realdf = sparkSession.createDataFrame(rdd)
      val schema = realdf.schema
      when(df.foreachPartition(any[ForeachPartitionFunction[Row]]()))
        .thenThrow(classOf[SQLException])
      when(df.sparkSession).thenReturn(sparkSession)
      when(df.schema).thenReturn(schema)
      when(df.rdd).thenReturn(realdf.rdd)

      // This would touch an exception, gptable are not created and temp table would be removed
      intercept[PartitionCopyFailureException](
        GreenplumUtils.copyOverwriteToGreenplum(df, schema, options))

      val showTables = "SELECT table_name FROM information_schema.tables"
      val stat = conn.createStatement()
      val result = stat.executeQuery(showTables)
      while (result.next()) {
        val tbl = result.getString(1)
        assert(tbl != tblname && !tbl.startsWith(tempPrefix))
      }
    }
  }

  def withConnectionAndOptions(f: (Connection, String, GreenplumOptions) => Unit ): Unit = {
    val paras =
      CaseInsensitiveMap(Map("url" -> s"$url", "delimiter" -> "\t", "dbtable" -> "gptest",
      "transactionForAppend" -> "true"))
    val options = GreenplumOptions(paras, timeZoneId)
    val conn = JdbcUtils.createConnectionFactory(options)()
    try {
      f(conn, options.table, options)
    } finally {
      val dropTbl = s"DROP TABLE IF EXISTS ${options.table}"
      GreenplumUtils.executeStatement(conn, dropTbl)
      GreenplumUtils.closeConnSilent(conn)
    }
  }
}
