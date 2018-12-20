package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.sql._
import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset
import org.apache.spark.sql.types.{StructType, StructField, StringType};
import org.apache.spark.sql.types.{StructType, StructField, LongType};

object csvtest {

  def main(args: Array[String]) ={
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
       val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()
  //sql context
  //val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
  import spark.implicits._

     val wrawdata=spark
    .read
       .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/Chicago_Ohare_International_Airport.csv")
    .repartition(6).toDF()

    val airlinesTable1=spark
      .read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/allyears2k_headers.csv.gz")
      .repartition(6).toDF()

    airlinesTable1.toDF.registerTempTable("FlightsToORD")
    wrawdata.toDF.registerTempTable("WeatherORD")

    //val kk= sqlContext.sql("select TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(w.date,'dd/MM/yyyy'),'yyyy-MM-dd')) AS dt, w.date from WeatherORD w")

    //register UDF function
    spark.udf.register("ksn",dateStr _)

    val bigTable = spark.sql(
      """SELECT
        |f.Year,f.Month,f.DayofMonth,
        |f.CRSDepTime,f.CRSArrTime,f.CRSElapsedTime,
        |f.UniqueCarrier,f.FlightNum,f.TailNum,
        |f.Origin,f.Distance,
        |w.TmaxF,w.TminF,w.TmeanF,w.PrcpIn,w.SnowIn,w.CDD,w.HDD,w.GDD,
        |f.ArrDelay
        |FROM FlightsToORD f
        |JOIN WeatherORD w
		    |ON
|TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(w.date,'dd/MM/yyyy'),
        |'yyyy-MM-dd')) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(ksn(f.Year,f.Month,f.DayofMonth),'yyyyMMdd'),'yyyy-MM-dd'))
        |WHERE f.ArrDelay IS NOT NULL""".stripMargin)
     
      bigTable.printSchema()
      bigTable.show()
    
}
  def dateStr(yr:Int, mon:Int, d:Int):String = {
    yr+""+mon+""+d
  }

}