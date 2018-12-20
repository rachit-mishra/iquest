package com.basic

import java.net.URL
import java.nio.charset.Charset
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Ravee on 8/18/2016.
  */
object spoolHDFSDir {

  def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

    if(args.length < 1) {
      System.err.println("Usage: <log-dir>")
      System.exit(1)
    }
    val inputDirectory = args(0)

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("DirectKafka")
      .getOrCreate()

    val sparkSessionContext =sparkSession.sparkContext
    val ssc = new StreamingContext(sparkSessionContext, Seconds(2))

    val lines = ssc.fileStream[LongWritable, Text, TextInputFormat](inputDirectory).map{ case (x, y) => (x.toString, y.toString) }

    lines.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
