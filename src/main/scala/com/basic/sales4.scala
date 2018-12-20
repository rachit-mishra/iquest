package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType};

//Assign column names to DataFrame
object sales4 {
  def main(args: Array[String]) ={

  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
  //spark 2
  val spark = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
    .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
    .getOrCreate()
//read csv file
val df1 = spark.read.csv("data/salex.txt")
//check schema
df1.printSchema()
//register as table
df1.registerTempTable("SalesTbl")
//get count
val df2 =spark.sql("select _c0 as dept,_c1 as roles from SalesTbl ")
//show count        
df2.show()
df2.printSchema()
print("Count"+df2.count())
//do group and show results
//val results = spark.sql("" +
 // "select dep, des, state, sum(cost) as mysum, count(*) cnt   from SalesTbl    group by dep,des,state")
//results.show
       
  }
}