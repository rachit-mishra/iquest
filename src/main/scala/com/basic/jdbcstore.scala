package com.basic

import org.apache.spark.sql.SparkSession

case class Harvest1 (org_id:String, projection: String, grid_id:String, comb_operation: String)

object jdbcstore {
  def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

  val driver = "com.mysql.jdbc.Driver"
  val url="jdbc:mysql://master:3306/customer"
  val prop = new java.util.Properties
  prop.setProperty("user","root")
  prop.setProperty("password","root")

  //spark 2
  val sparkSession = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .config("spark.some.config.option", "config-value")
                          .getOrCreate()
  import sparkSession.implicits._
  val hdfsdataset=sparkSession.read.text("/salex.txt").toDF()
    //write HDFS data to MYSQL
    hdfsdataset.write.mode("append").jdbc(url, "harvest", prop)
  }
}