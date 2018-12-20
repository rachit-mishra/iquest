package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}

object task22 {
  case class flights(flightnum: Int, year: String, day: String, month: String, deparloc: String, deprDelay: Int, ArrDelay: String)

  def main(args: Array[String]): Unit = {
      System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

      //spark 2
      val conf = new SparkConf().setAppName("Testing").setMaster("local")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val data = sc.textFile("data/flight.txt")

    import sqlContext.implicits._
    val dfdata = data.map(line => line.split(",")).map(row => flights(row(0).toInt, row(1), row(2), row(3), row(4), row(5).toInt, row(6)))

    dfdata.toDF().registerTempTable("dfdatatbl")

    //val finaldata = sqlContext.sql("select flightnum,year,day,month,deparloc,derDelay,ArrDelay from dfdatatbl " +
    //  "where derDelay>=10 order by derDelay asc")

    //finaldata.map(line => line.mkString(",")).repartition(1).saveAsTextFile("/data/flight_task")
  }
}