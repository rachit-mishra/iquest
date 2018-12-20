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

object readJson {
  

 def main(args: Array[String]) ={
   
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");  
  //spark 2
  val sparkSession = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .config("spark.some.config.option", "config-value")
                          .getOrCreate()
     
  // this is used to implicitly convert an RDD to a DataFrame.
     import sparkSession.implicits._
     val erorDF = sparkSession.read.json("error.json")
     erorDF.printSchema()
     erorDF.registerTempTable("errorTbl")
     erorDF.show()
     sparkSession.sql("select rowkey from errorTbl").show()

 
  }
}