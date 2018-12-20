package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset
import org.apache.spark.sql.types.{StructType, StructField, StringType};
import org.apache.spark.sql.types.{StructType, StructField, LongType};


object Tutorial {
      def main(args: Array[String]): Unit = {

          System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
        //spark 2
        val spark = SparkSession.builder
          .master("local")
          .appName("my-spark-app")
          .config("spark.some.config.option", "config-value")
          .getOrCreate()
       import spark.implicits._

        val ErrorData = spark.read.json("error.json")
        ErrorData.printSchema()
        ErrorData.show()
        //show one column
        ErrorData.select("rowkey").show()


  }
}