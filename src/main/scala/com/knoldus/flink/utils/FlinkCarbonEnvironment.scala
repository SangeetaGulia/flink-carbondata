/*
package com.knoldus.flink.utils

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.operators.DataSource
import org.apache.spark.sql.{DataFrame, SparkSession}

object FlinkCarbonEnvironment extends ExecutionEnvironment {

  def getEnvironment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  override def getExecutionPlan = super.getExecutionPlan

  override def startNewSession() = super.startNewSession()

  override def execute(s:String): JobExecutionResult = super.execute(s)

  def readFromCarbon(sparkSession: SparkSession, query : String): DataFrame = sparkSession.sql(query)

}
*/
