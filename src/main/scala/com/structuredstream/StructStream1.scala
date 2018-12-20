package com.structuredstream

import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ StructType, StructField, StringType }
import org.apache.spark.sql.types.{ StructType, StructField, IntegerType }

object StructStream1 {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    // Create context with 10 second batch interval
    val spark = SparkSession.builder
      .master("local")
      .appName("StructStream1")
      .config("checkpoint", "checkpoint")
      .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._
    import scala.concurrent.duration._

    ///IMPORTANT: you need to define schema for your input data.
    // Otherwise  readStream will through error.
    //Spend 4 hrs to fix this issue. Documentation was not clear.
    val userSchema = new StructType().add("id", "String").add("name", "String").add("age", "Integer")
    val errorStream = spark.readStream
      .schema(userSchema)
      .json("/age")

    val result = errorStream.writeStream.format("parquet")
      .option("checkpointLocation", "checkpoint").option("mode", "append").start("/data1/er/")

    result.awaitTermination()

  }

}