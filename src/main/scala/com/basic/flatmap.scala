package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StructType, StructField, StringType }

object flatmap {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    val spark = SparkSession
      .builder
      .master("local")
      .appName("MyerrorsTesing-app")
      .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val salesData = sc.textFile("data/salex.txt")
    // salesData.toDF.show()

    val salesflatMap = salesData.flatMap(line => line.split(','))
    val flatMap = salesflatMap.toDF("col1")
    // flatMap.count()
    val salesMap = salesData.map(line => line.split(','))
    val df1 = salesMap.toDF()
    df1.printSchema()
    //flatMap.registerTempTable("flatMapTbl")
    sqlContext.sql("select count(*), col1 from flatMapTbl group by col1").show()

  }
}