package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}

//Spark 2. Use seq to build person class and convert to DataSet

object person {
case class Person(name: String, age: Int)

  def main(args: Array[String]) ={
     
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

  //spark 2
  val sparkSession = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .config("spark.some.config.option", "config-value")
                          .getOrCreate()
import sparkSession.implicits._

val personDS = Seq(Person("Max", 33), Person("Adam", 32), Person("Muller", 62)).toDS()

personDS.show()


  }
}