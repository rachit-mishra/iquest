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

object testing {
  def main(args: Array[String]) ={
   
    //http://blog.antlypls.com/blog/2016/01/30/processing-json-data-with-sparksql/
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");    
  //spark 2
  val spark = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .config("spark.some.config.option", "config-value")
                          .getOrCreate()
  val sc = spark.sparkContext

  val erorDF1 = sc.textFile("error.json").map (line => line.toUpperCase())
 val cnt = erorDF1.flatMap ( line => line.split(":"))
  .map ( word =>(word,1)).reduceByKey(_+_).foreach(println)
  
  //=========================
  // for (erorDF1 <- erorDF1.take(200))
  //  println(erorDF1)
  // }

  //================================
// val a = sc.parallelize(List(1, 2, 1, 3), 1)
// val b = erorDF1.zip(erorDF1)
//  b.foreach(println)
  
 }

}