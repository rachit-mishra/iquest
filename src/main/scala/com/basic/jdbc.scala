package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType};

case class Harvest (org_id:String, projection: String, grid_id:String, comb_operation: String)

object jdbc {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://master:3306/customer"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    //spark 2
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()
    import sparkSession.implicits._
    //read data from MYSQL
    val custRelDF = sparkSession.read.format("jdbc")
      .options(
        Map("driver" -> "com.mysql.jdbc.Driver",
          "url" -> "jdbc:mysql://master:3306/customer",
          "user" -> "root",
          "password" -> "root",
          "dbtable" -> "harvest")
      )
      .load()
      .toDF()
    custRelDF.show()
  }
}