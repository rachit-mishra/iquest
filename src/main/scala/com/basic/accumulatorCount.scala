package com.basic

import com.basic.sales3.Employee
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession

/**
 * Created by Ravee on 8/20/2016.
 */
object accumulatorCount {

  case class Employee(dep: String, des: String, cost: Double, state: String)

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

    //spark 2
    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()

    val conf = new SparkConf().setAppName("Testing").setMaster("local")
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import spark.implicits._
    val nErrors = sc.accumulator(0.0)
    val logs = sc.textFile("output.log")
    logs.filter(line => line.contains("error")).foreach(x => nErrors += 1)
    nErrors.value

  }

}
