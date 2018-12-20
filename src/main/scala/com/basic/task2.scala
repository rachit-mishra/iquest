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

object task2 {
  def main(args: Array[String])={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
      val spark = SparkSession
      .builder
      .master("local")
      .appName("MyerrorsTesing-app")
      .getOrCreate()

 // this is used to implicitly convert an RDD to a DataFrame.
 import spark.implicits._

    val data =spark.read.json("/user/horton/task2")
        data.toDF().createOrReplaceTempView("dfdatatbl")
    val finaldata= spark.sql("select state,avg(cast (balance as double)) from dfdatatbl where balance>2000 group by state order by state asc")
    finaldata.show()
   //finaldata.map(line=>line.mkString("\t")).repartition(1).saveAsTextFile("/user/horton/solutions/task2")

  }

}