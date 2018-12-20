package com.basic

import org.apache.spark.sql.SparkSession

/**
  * Created by Ravee on 12/2/2016.
  */
object FunctionObject {

  def main(args: Array[String])={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    val spark = SparkSession
      .builder
      .master("local")
      .appName("MyerrorsTesing-app")
      .getOrCreate()
    import spark.implicits._

    val date= "01-01-2016"
    val time ="7:00AM"
    val tz="CST"
    val dt =makeDT(date,time,tz)
    print(dt)
    val dt2 =makeDT2(makeDT,date,time,tz)
    print(dt2)
    val dt3 =makeDT2(makeDTNew,date,time,tz)
    print(dt3)

  }

  def makeDT(date: String, time: String, tz: String) = {
     s"$date $time $tz"
  }
  def makeDTNew(date: String, time: String, tz: String) = s"$time $tz"

  def makeDT2(fskksk:(String,String,String)=>String,
              date: String, time: String, tz: String):String = {
    fskksk(date,time, tz)+"testing"
  }

  def makeDT3(    date: String, time: String, tz: String):String = {
    makeDT(date,time, tz)+"NEwtesting"
  }


}
