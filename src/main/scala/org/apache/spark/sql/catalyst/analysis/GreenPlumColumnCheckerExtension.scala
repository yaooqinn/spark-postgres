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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand
import org.apache.spark.sql.execution.datasources.greenplum.GreenplumRelation

case class GreenPlumColumnChecker(spark: SparkSession) extends (LogicalPlan => Unit) with Logging {
  override def apply(plan: LogicalPlan): Unit = {
    if (plan.isInstanceOf[InsertIntoDataSourceCommand]) {
      val command = plan.asInstanceOf[InsertIntoDataSourceCommand]
      val logicRelation = command.logicalRelation
      val baseRelation = logicRelation.relation
      if (baseRelation.isInstanceOf[GreenplumRelation]) {
        val relationOutput = logicRelation.output
        if (command.query.isInstanceOf[Project]) {
          // This is the real output of sub query, which is not be casted.
          val realOutput = command.query.asInstanceOf[Project].child.output
          if (relationOutput.size != realOutput.size ||
          relationOutput.zip(realOutput).exists(ats => ats._1.name != ats._2.name)) {
            throw new AnalysisException(
              s"""
                 | The column names of GreenPlum table are not consistent with the
                 | projects output names of subQuery.
             """.stripMargin)
          }
        }
        logInfo(s"GreenPlumColumnChecker: check passed.")
      }
    }
  }
}

class GreenPlumColumnCheckerExtension extends (SparkSessionExtensions => Unit) {
  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectCheckRule(GreenPlumColumnChecker)
  }
}
