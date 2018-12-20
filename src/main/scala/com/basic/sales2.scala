package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType};


//Assign column names to DataFrame
object sales2 {
  def main(args: Array[String]) ={
     
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

  //spark 2
  val sparkSession = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .config("spark.some.config.option", "config-value")
                          .getOrCreate()
  
//data frame column names
val newNames = Seq("dep", "des", "cost", "state")
//read csv file
val df = sparkSession.read.csv("salex.txt")
//set column names
val df2= df.toDF(newNames: _*)
//check schema
df2.printSchema()
//check data
df2.show()  
//register as table
df2.registerTempTable("SalesTbl")
//get count
val df4 =sparkSession.sql("select count(*) from SalesTbl")
//show count        
df4.show()

//do group and show results
val results = sparkSession.sql("select dep, des, state, sum(cost), count(*)   from SalesTbl    group by dep,des,state")
results.show
       
  }
}