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
import java.util.concurrent.{TimeoutException, TimeUnit}

import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import scala.concurrent.Promise

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
      length: Int,
      delimiter: String,
      valueConverters: Array[(Row, Int) => String]): Array[Byte] = {
    var i = 0
    val values = new Array[String](length)
    while (i < length) {
      if (!row.isNullAt(i)) {
        values(i) = convertValue(valueConverters(i).apply(row, i), delimiter.charAt(0))
      } else {
        values(i) = "NULL"
      }
      i += 1
    }
    (values.mkString(delimiter) + "\n").getBytes("UTF-8")
  }

  def convertValue(str: String, delimiter: Char): String = {
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
   * Copy data to greenplum in a single transaction.
   *
   * @param df the [[DataFrame]] will be copy to the Greenplum
   * @param schema the table schema in Greemnplum
   * @param options Options for the Greenplum data source
   */
  def transactionalCopy(
      df: DataFrame,
      schema: StructType,
      options: GreenplumOptions): Unit = {
    val randomString = UUID.randomUUID().toString.filterNot(_ == '-')
    val suffix = "sparkGpTmp"
    val tempTable = s"${options.table}_${randomString}_$suffix"
    val strSchema = JdbcUtils.schemaString(df, options.url, options.createTableColumnTypes)
    val createTempTbl = s"CREATE TABLE $tempTable ($strSchema) ${options.createTableOptions}"

    val conn = JdbcUtils.createConnectionFactory(options)()

    try {
      executeStatement(conn, createTempTbl)
      val accumulator = df.sparkSession.sparkContext.longAccumulator("copySuccess")
      val partNum = df.rdd.getNumPartitions

      df.foreachPartition { rows =>
        copyPartition(rows, options, schema, tempTable, Some(accumulator))
      }

      if (accumulator.value == partNum) {
        val dropTbl = s"DROP TABLE IF EXISTS ${options.table}"
        executeStatement(conn, dropTbl)

        val renameTempTbl = s"ALTER TABLE $tempTable RENAME TO ${options.table}"
        executeStatement(conn, renameTempTbl)
      } else {
        var retryCount = 0
        var dropSuccess = false
        val dropTempTbl = s"DROP TABLE $tempTable"
        try {
          while (!dropSuccess && retryCount < options.dropTempTableMaxRetries) {
            try {
              executeStatement(conn, dropTempTbl)
              dropSuccess = true
            } catch {
              case _: Exception => retryCount += 1
            }
          }
        } finally {
          if (!dropSuccess) {
            logError(s"Failed to drop the temp table: $tempTable, you can drop it manually.")
          }
          throw new PartitionCopyFailureException(
            s"""
               | Job aborted for that there are some partitions failed to copy data to greenPlum:
               | Total partitions is: ${partNum} and successful partitions is: ${accumulator.value}.
               | You can retry again.
            """.stripMargin)
        }
      }
    } finally {
      closeConnSilent(conn)
    }
  }

  /**
   * https://www.postgresql.org/docs/9.2/sql-copy.html
   *
   * Copy data to greenplum in these cases, which need update origin gptable.
   * 1. Overwrite an existed gptable, which is a CascadingTruncateTable.
   * 2. Append data to a gptable.
   *
   * When transcationOn option is true, we will coalesce the dataFrame to one partition,
   * and the copy operation for each partition is atomic.
   *
   * @param df the [[DataFrame]] will be copy to the Greenplum
   * @param schema the table schema in Greemnplum
   * @param options Options for the Greenplum data source
   */
  def nonTransactionalCopy(
      df: DataFrame,
      schema: StructType,
      options: GreenplumOptions): Unit = {
    df.foreachPartition { rows =>
      copyPartition(rows, options, schema, options.table)
    }
  }

  /**
   * Copy a partition's data to a gptable.
   *
   * @param rows rows of a partition will be copy to the Greenplum
   * @param options Options for the Greenplum data source
   * @param schema the table schema in Greemnplum
   * @param tableName the tableName, to which the data will be copy
   * @param accumulator account for recording the successful partition num
   */
  def copyPartition(
      rows: Iterator[Row],
      options: GreenplumOptions,
      schema: StructType,
      tableName: String,
      accumulator: Option[LongAccumulator] = None): Unit = {
    val valueConverters: Array[(Row, Int) => String] =
      schema.map(s => makeConverter(s.dataType, options)).toArray
    val conn = JdbcUtils.createConnectionFactory(options)()
    val copyManager = new CopyManager(conn.asInstanceOf[BaseConnection])

    try {
      val tmpDir = Utils.createTempDir(Utils.getLocalDir(SparkEnv.get.conf), "greenplum")
      val dataFile = new File(tmpDir, UUID.randomUUID().toString)
      val out = new BufferedOutputStream(new FileOutputStream(dataFile))
      try {
        rows.foreach(r => out.write(
          convertRow(r, schema.length, options.delimiter, valueConverters)))
      } finally {
        out.close()
      }
      val in = new BufferedInputStream(new FileInputStream(dataFile))
      val sql = s"COPY $tableName" +
        s" FROM STDIN WITH NULL AS 'NULL' DELIMITER AS E'${options.delimiter}'"

      @volatile
      var copyException: Option[Throwable] = None
      val promisedCopyNums = Promise[Long]
      val copyThread = new Thread("copy-to-gp-thread") {
        override def run(): Unit = {
          try {
            promisedCopyNums.trySuccess {
              copyManager.copyIn(sql, in)
            }
          } catch {
            case e: Exception => copyException = Some(e)
          }
        }
      }

      val timeout = TimeUnit.MILLISECONDS.toNanos(options.copyTimeout)
      def checkCopyThread(start: Long): Boolean = {
        copyException.isEmpty && System.nanoTime() - start < timeout &&
          !promisedCopyNums.isCompleted
      }

      try {
        logInfo("Start copy steam to Greenplum")
        val start = System.nanoTime()
        copyThread.start()
        while (checkCopyThread(start)) {
          Thread.sleep(1000)
        }
        copyException.foreach(e => throw e)
        if (!promisedCopyNums.isCompleted) {
          throw new TimeoutException(
            s"""
               | The copy operation for copying this partition's data to greenplum has been running for
               | more than the timeout: ${TimeUnit.NANOSECONDS.toSeconds(options.copyTimeout)}s.
               | You can configure this timeout with option copyTimeout, such as "2h", "100min",
               | and default copyTimeout is "1h".
            """.stripMargin)
        }
        val nums = promisedCopyNums.future.value
        val end = System.nanoTime()
        logInfo(s"Copied $nums row(s) to Greenplum," +
          s" time taken: ${(end - start) / math.pow(10, 9)}s")
        accumulator.foreach(_.add(1L))
      } finally {
        copyThread.interrupt()
        copyThread.join()
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

  def executeStatement(conn: Connection, sql: String): Unit = {
    val statement = conn.createStatement()
    try {
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }
  }
}
