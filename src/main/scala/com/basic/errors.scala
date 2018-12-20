package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.sql._
import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset
import org.apache.spark.sql.types.{StructType, StructField, StringType};
import org.apache.spark.sql.types.{StructType, StructField, LongType};

object errors {
  def main(args: Array[String])={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
      val spark = SparkSession
      .builder
      .master("local")
      .appName("MyerrorsTesing-app")
        .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
    .getOrCreate()

 // this is used to implicitly convert an RDD to a DataFrame.
 import spark.implicits._

 var erorDF1= spark.read.json("hdfs://master:8020/data/error.json")
 erorDF1.show()
 erorDF1.printSchema()
 erorDF1.createOrReplaceTempView("erorDFTBL")
 var df2= spark.sql("select count(*) from erorDFTBL where rate is null")
 df2.show()

  }

}