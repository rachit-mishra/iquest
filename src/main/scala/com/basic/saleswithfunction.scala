package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}
case class ebayData(dep: String, des: String, cost: Double, state: String)

object saleswithfunction {

  def main(args: Array[String]) ={
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    val spark = SparkSession
      .builder
      .master("local")
      .appName("MyerrorsTesing-app")
      .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    
    val ebayText = sc.textFile("data/ebay.txt")
    val saleDataRDDarray =ebayText.map(line =>line.split(","))

    val dfRDD =saleDataRDDarray.map(
      row =>ebayData(row(0), row(1), row(2).toDouble, row(7)      )
    )
    //dfRDD.toDF().printSchema()

   val salesempDF = ebayText.map(line =>line.split(","))
    .map(row=>getEmployee1(row) )
    salesempDF.toDF().show()
  //  val salesempDF1 = ebayText.map(_.split(",")).map(getEmployee1 ).toDF()
  //  salesempDF1.show()
}

 def getEmployee1(row:Array[String]):ebayData={

   ebayData(row(0), row(1), row(2).toDouble, row(3) )
   }

}