package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StructType, StructField, StringType }
import org.apache.spark.sql._
import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset

object bank {

  //make sure to register this outside of main
  case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)

  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()

    val bankText = sparkSession.sparkContext.parallelize(IOUtils.toString(
      new URL("https://s3.amazonaws.com/apache-zeppelin/tutorial/bank/bank.csv"),
      Charset.forName("utf8")).split("\n"))

    // this is used to implicitly convert an RDD to a DataFrame.
    import sparkSession.implicits._

    val bank = bankText.map(s => s.split(";")).filter(s => s(0) != "\"age\"").map(
      s => Bank(s(0).toInt,
        s(1).replaceAll("\"", ""),
        s(2).replaceAll("\"", ""),
        s(3).replaceAll("\"", ""),
        s(5).replaceAll("\"", "").toInt))

    bank.toDF().show()

    bank.toDF().createOrReplaceTempView("bank")
    //sparkSession.sql("select age, count(1) value from bank where age < 30 group by age order by age").collect.foreach(println)

  }

}