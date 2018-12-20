package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}

object sales3 {

  case class Employee(dep: String, des: String, cost: Double, state: String)
  
    def main(args: Array[String]) ={
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

  //spark 2
  val sparkSession = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .config("spark.some.config.option", "config-value")
                          .getOrCreate()


val salesemp = sparkSession.read.csv("/uesr/ubnt/person")
import sparkSession.implicits._
//val salesempDF = salesemp.map(_.split(",")).map( row => Employee(row(0), row(1), row(2).toDouble, row(3) )).toDF()
  // salesempDF.registerTempTable("table")
   // val cint  =sparkSession.sql("select count(*) from table")
     //cint.printSchema()
      //print(cint)















      //val salesempDF1 = salesemp.map(_.split(",")).map( row => Employee(row(0), row(1), row(2).toDouble, row(3) )).as[Employee]

//convert DF to Dataset
//val dataset = salesempDF.as[Employee]

//Convert DS to table
//salesempDF1.registerTempTable("salesempDFTb")
//use table to RUN adhoc queries
//val results = sparkSession.sql("select dep, des, state, sum(cost), count(*)   from salesempDFTbl    group by dep,des,state")
//results.show

}
  
}  