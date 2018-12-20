package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType};

object udftest {

  case class Purchase(customer_id: Int, purchase_id: Int, date: String, time: String, tz: String, amount:Double)


  def main(args: Array[String]):Unit={
     
     System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

    val spark= SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._

    val x = spark.sparkContext.parallelize(Array(
      Purchase(123, 234, "2007-12-12", "20:50", "UTC", 500.99),
      Purchase(123, 247, "2007-12-12", "15:30", "PST", 300.22),
      Purchase(189, 254, "2007-12-13", "00:50", "EST", 122.19),
      Purchase(187, 299, "2007-12-12", "07:30", "UTC", 524.37)
    ))
    val df = spark.createDataFrame(x)
    df.registerTempTable("df")
    import org.apache.spark.sql.functions.udf


    val makeDt = udf(makeDT(_:String,_:String,_:String))
    // now this works
   df.select($"customer_id", makeDt($"date", $"time", $"tz"), $"amount").take(2)

    //
   spark.udf.register("satya", makeDT(_:String,_:String,_:String))
   spark.sql("SELECT amount, satya(date, time, tz) from df").take(2)

    import org.apache.spark.sql.functions.unix_timestamp

    val fmt = "yyyy-MM-dd hh:mm z"
   df.select($"customer_id", unix_timestamp(makeDt($"date", $"time", $"tz"), fmt), $"amount").take(2)
   spark.sql(s"SELECT customer_id, unix_timestamp(makeDt(date, time, tz), '$fmt'), amount FROM df").take(2)

  }

  def makeDT(date: String, time: String, tz: String) = s"$date $time $tz"

}