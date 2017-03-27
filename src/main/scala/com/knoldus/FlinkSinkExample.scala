package com.knoldus

import java.io.File

import org.apache.spark.sql.DataFrame
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.spark.sql.SparkSession
object FlinkSinkExample extends App{

  val rootPath = new File(this.getClass.getResource("/").getPath
    + "../../../..").getCanonicalPath
  val storeLocation = s"$rootPath/target/store"
  val warehouse = s"$rootPath/target/warehouse"
  val metastoredb = s"$rootPath/target"

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


  // scalastyle:off
  spark.sql(
    s"""
       | LOAD DATA LOCAL INPATH '$path'
       | INTO TABLE carbon_table
       | options('FILEHEADER'='shortField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData','COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
  // scalastyle:on

  val dataframe: DataFrame = spark.sql("""
             SELECT *
             FROM carbon_table
             where stringfield = 'spark' and decimalField > 40
              """)
  dataframe.show()

  val env = ExecutionEnvironment.getExecutionEnvironment



  // Drop table
  spark.sql("DROP TABLE IF EXISTS carbon_table")


}
