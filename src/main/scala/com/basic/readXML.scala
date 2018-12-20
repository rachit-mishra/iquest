package com.basic

import org.apache.spark.sql.SparkSession

object readXML {

  def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._

    val df =spark.read
      .format("com.databricks.spark.xml")
      .option("rootTag", "dailyTlmtcRptFile")
      .option("rowTag", "rawVndrTlmtcRptDetl")
      .load("data/TelematicReport2.xml")

    df.printSchema()
   df.createOrReplaceTempView("dailysummary")
    val df2= spark.sql("select spdIntvl.spdIntvlNm._VALUE " +
      " from dailysummary ")
    df2.show()
   //     df.coalesce(1).write.mode("append").json("/user/data/sales7")
  }
}
