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

object Tweet {
  def main(args: Array[String])={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
      val spark = SparkSession
      .builder
      .master("local")
      .appName("MyerrorsTesing-app")
    .getOrCreate()

    import spark.implicits._
    val tweet= spark.read.json("/data/Tweets.json")
    tweet.write.mode("append").parquet("/user/hive/warehouse/tweet_parquet")
  }

}