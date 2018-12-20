package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}
object RemoveHeads_1 {
  
  case class Employee(dep: String, des: String, cost: String)
    
  def main(args: Array[String]): Unit = {
    
     System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
      //spark 2
      val spark = SparkSession
        .builder
        .master("local")
        .appName("MyerrorsTesing-app")
        .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
        .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._
                           
    // Read the CSV file
    val Files = "data/headers/*.csv"
    val csv = sc.textFile(Files)
    val first = "instant,dteday,season,yr,mnth,hr,holiday,weekday,workingday,weathersit,temp,atemp,hum,windspeed,casual,registered,cnt"
    val salesempDF = csv.filter(x => x!=first)
    salesempDF.toDF().show()

  }
}