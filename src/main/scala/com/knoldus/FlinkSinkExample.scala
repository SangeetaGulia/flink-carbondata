package com.knoldus

import java.io.File
import java.util

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hadoop.{CarbonInputFormat, CarbonProjection}
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{DataFrame, SparkSession}


object FlinkSinkExample extends App{

  val rootPath = new File(this.getClass.getResource("/").getPath
    + "../../../..").getCanonicalPath
  val storeLocation = s"$rootPath/target/store"
  val warehouse = s"$rootPath/target/warehouse"
  val metastoredb = s"$rootPath/target"

  println(s"storelocation is $storeLocation")

  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

  import org.apache.spark.sql.CarbonSession._
  val spark = SparkSession.builder().master("local")
    .appName("CarbonDataFrameExample")
    .config("spark.sql.warehouse.dir", warehouse)
    .getOrCreateCarbonSession(storeLocation, metastoredb)

  spark.sparkContext.setLogLevel("ERROR")

  spark.sql("DROP TABLE IF EXISTS carbon_table")

  // Create table
  spark.sql(
    s"""
       | CREATE TABLE carbon_table(
       |    shortField short,
       |    intField int,
       |    bigintField long,
       |    doubleField double,
       |    stringField string,
       |    timestampField timestamp,
       |    decimalField decimal(18,2),
       |    dateField date,
       |    charField char(5),
       |    floatField float,
       |    complexData array<string>
       | )
       | STORED BY 'carbondata'
       | TBLPROPERTIES('DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

  val path = "/home/sangeeta/projects/contribute/flink-carbondata/src/main/resources/data.csv"

  spark.sql(
    s"""
       | LOAD DATA LOCAL INPATH '$path'
       | INTO TABLE carbon_table
       | options('FILEHEADER'='shortField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData','COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)


  val dataframe: DataFrame = spark.sql("""SELECT * FROM carbon_table""")
  dataframe.show()

  val env = ExecutionEnvironment.getExecutionEnvironment
  val configuration = new Configuration()

  val projection = new CarbonProjection
  projection.addColumn("shortField")  // column c1
  projection.addColumn("intField")  // column c3
  projection.addColumn("bigintField")  // column c3

  CarbonInputFormat.setColumnProjection(configuration, projection)

  val data: DataSource[Tuple2[Void, Array[Object]]] = env.readHadoopFile(
    new CarbonInputFormat[Array[Object]],
    classOf[Void],
    classOf[Array[Object]],
    "/home/sangeeta/projects/target/store/default/carbon_table",
    new Job(configuration))

  val result: util.List[Tuple2[Void, Array[Object]]] = data.collect()
  println("\n\n\n>>>>>>>>>>>>>>>>>" + result)
  for (i <- 0 until result.size()) {
    println(result.get(i).f1.mkString(","))
  }

  // Drop table
  spark.sql("DROP TABLE IF EXISTS carbon_table")

}
