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

import java.io._
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.util.UUID

import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.{LongAccumulator, Utils}

object GreenplumUtils extends Logging {

  def makeConverter(
      dataType: DataType,
      options: GreenplumOptions): (Row, Int) => String = dataType match {
    case StringType => (r: Row, i: Int) => r.getString(i)
    case BooleanType => (r: Row, i: Int) => r.getBoolean(i).toString
    case ByteType => (r: Row, i: Int) => r.getByte(i).toString
    case ShortType => (r: Row, i: Int) => r.getShort(i).toString
    case IntegerType => (r: Row, i: Int) => r.getInt(i).toString
    case LongType => (r: Row, i: Int) => r.getLong(i).toString
    case FloatType => (r: Row, i: Int) => r.getFloat(i).toString
    case DoubleType => (r: Row, i: Int) => r.getDouble(i).toString
    case DecimalType() => (r: Row, i: Int) => r.getDecimal(i).toString

    case DateType =>
      (r: Row, i: Int) => options.dateFormat.format(DateTimeUtils.toJavaDate(r.getInt(i)))

    case TimestampType => (r: Row, i: Int) =>
      options.timestampFormat.format(DateTimeUtils.toJavaTimestamp(r.getLong(i)))

    case BinaryType => (r: Row, i: Int) =>
      new String(r.getAs[Array[Byte]](i), StandardCharsets.UTF_8)

    case udt: UserDefinedType[_] => makeConverter(udt.sqlType, options)
    case _ => (row: Row, ordinal: Int) => row.get(ordinal).toString
  }

  def convertRow(
      row: Row,
      schema: StructType,
      options: GreenplumOptions,
      valueConverters: Array[(Row, Int) => String]): Array[Byte] = {
    var i = 0
    val values = new Array[String](schema.length)
    while (i < schema.length) {
      if (!row.isNullAt(i)) {
        values(i) = convertValue(valueConverters(i).apply(row, i), options)
      } else {
        values(i) = "NULL"
      }
      i += 1
    }
    (values.mkString(options.delimiter) + "\n").getBytes("UTF-8")
  }

  def convertValue(str: String, options: GreenplumOptions): String = {
    assert(options.delimiter.length == 1, "The delimiter should be a single character.")
    val delimiter = options.delimiter.charAt(0)
    str.flatMap {
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case `delimiter` => s"\\$delimiter"
      case c => s"$c"
    }
  }

  /**
   * https://www.postgresql.org/docs/9.2/sql-copy.html
   *
   * Copy data to greenplum and append to relative gptable.
   *
   * @param df the [[DataFrame]] will be copy to the Greenplum
   * @param schema the table schema in Greemnplum
   * @param options Options for the Greenplum data source
   */
  def copyAppendToGreenplum(
      df: DataFrame,
      schema: StructType,
      options: GreenplumOptions): Unit = {
    df.foreachPartition { rows =>
      copyParition(rows, options, schema, options.table)
    }
  }

  /**
   * https://www.postgresql.org/docs/9.2/sql-copy.html
   *
   * Copy data to greenplum and overwrite relative gptable,
   * which can be dropped or does not exist.
   *
   * @param df the [[DataFrame]] will be copy to the Greenplum
   * @param schema the table schema in Greemnplum
   * @param options Options for the Greenplum data source
   */
  def copyOverwriteToGreenplum(
      df: DataFrame,
      schema: StructType,
      options: GreenplumOptions): Unit = {
    val randomString = UUID.randomUUID().toString.flatMap {
      case '-' => ""
      case c => s"$c"
    }
    val tempTable = s"sparkGpTmp$randomString"
    val strSchema = JdbcUtils.schemaString(df, options.url, options.createTableColumnTypes)
    val createTempTbl = s"CREATE TABLE $tempTable ($strSchema) ${options.createTableOptions}"

    val conn = JdbcUtils.createConnectionFactory(options)()

    try {
      executeStatement(conn, createTempTbl)
      val accumulator = df.sparkSession.sparkContext.longAccumulator("copySuccess")
      val partNum = df.rdd.getNumPartitions

      df.foreachPartition { rows =>
        copyParition(rows, options, schema, tempTable, accumulator)
      }

      if (accumulator.value == partNum) {
        val dropTbl = s"DROP TABLE IF EXISTS ${options.table}"
        executeStatement(conn, dropTbl)

        val renameTempTbl = s"ALTER TABLE $tempTable RENAME TO ${options.table}"
        executeStatement(conn, renameTempTbl)
      } else {
        val dropTempTbl = s"DROP TABLE $tempTable"
        executeStatement(conn, dropTempTbl)
      }
    } finally {
      closeConnSilent(conn)
    }
  }

  def copyParition(
      rows: Iterator[Row],
      options: GreenplumOptions,
      schema: StructType,
      tableName: String,
      accumulator: LongAccumulator = null): Unit = {
    val valueConverters: Array[(Row, Int) => String] =
      schema.map(s => makeConverter(s.dataType, options)).toArray
    val conn = JdbcUtils.createConnectionFactory(options)()
    val copyManager = new CopyManager(conn.asInstanceOf[BaseConnection])

    try {
      val tmpDir = Utils.createTempDir(Utils.getLocalDir(SparkEnv.get.conf), "greenplum")
      val dataFile = new File(tmpDir, UUID.randomUUID().toString)
      val out = new BufferedOutputStream(new FileOutputStream(dataFile))
      try {
        rows.foreach(r => out.write(convertRow(r, schema, options, valueConverters)))
      } finally {
        out.close()
      }
      val in = new BufferedInputStream(new FileInputStream(dataFile))
      val sql = s"COPY $tableName" +
        s" FROM STDIN WITH NULL AS 'NULL' DELIMITER AS E'${options.delimiter}'"
      try {
        logInfo("Start copy steam to Greenplum")
        val start = System.nanoTime()
        val nums = copyManager.copyIn(sql, in)
        val end = System.nanoTime()
        logInfo(s"Copied $nums row(s) to Greenplum," +
          s" time taken: ${(end - start) / math.pow(10, 9)}s")
        if (accumulator != null) {
          accumulator.add(1L)
        }
      } finally {
        in.close()
      }
    } finally {
      closeConnSilent(conn)
    }
  }

  def closeConnSilent(conn: Connection): Unit = {
    try {
      conn.close()
    } catch {
      case e: Exception => logWarning("Exception occured when closing connection.", e)
    }
  }

  def executeStatement(conn: Connection, sql: String): Boolean = {
    val statement = conn.createStatement()
    try {
      statement.executeUpdate(sql)
      true
    } finally {
      statement.close()
    }
  }
}
