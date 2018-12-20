package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType};

//Old style spark conf and SQL context
//use StructType to asign Columns to RDD.
object sales {
  
  case class Employee(dep: String, des: String, cost: String, state: String)
  
    def main(args: Array[String]) ={
     
     System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

    val conf = new SparkConf().setAppName("Testing").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   import sqlContext.implicits._
  
  val schemaString = "dep des cost state"
  val people = sc.textFile("salex.txt")

  val schema =
    StructType(
            schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

// Convert records of the RDD (people) to Rows.
val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1), p(2),p(3) ))

// Apply the schema to the RDD.
val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
peopleDataFrame.show()
        
  }
}