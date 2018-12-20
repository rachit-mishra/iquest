package com.basic

import org.apache.spark.sql.SparkSession

/**
  * Created by Ravee on 12/8/2016.
  */
object rowsToColumns {

  def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

    //spark 2
    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.sql.warehouse.dir","file:///C:/Users/Ravee/IdeaProjects/Testing/spark-warehous")
      .getOrCreate()

    val rowsDF = spark
      .read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/rows.txt")

    rowsDF.show()
    rowsDF.createOrReplaceTempView("rows")
    val df2= spark.sql("select row,collect_set(col) from rows group by row")
    df2.show()
  }

}