package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}
object RemoveHead {
  
  case class Employee(dep: String, des: String, cost: String)
    
  def main(args: Array[String]): Unit = {
    
     System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
      //spark 2
      val conf = new SparkConf().setAppName("Testing").setMaster("local")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
       import sqlContext.implicits._
                           
    // Read the CSV file
    val csv = sc.textFile("files.txt")
   // val first = csv.first()
    val df22 =csv.filter { x => x!=csv.first() }
      
    val salesempDF = df22.map(_.split(",")).map( row => Employee(row(0), row(1), row(2) )).toDF()
    
    //all rows including header
    salesempDF.show()
    //////////////////////////////////////////////////////////
    //get header row
   // val headerDF = salesempDF.first()
    //filter header row
   // val finalDF =salesempDF.filter(row =>row!=headerDF) 
    //finalDF.show()
    ////////////////////////////////////////////////////////////////////////

  }
}