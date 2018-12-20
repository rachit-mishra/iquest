package com.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ravee on 8/20/2016.
  */
object broadCast2 {

  case class Item1(id:String, name:String, unit:Int, companyId:String)
  case class Company1(companyId:String, name:String, city:String)

  def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

    val conf = new SparkConf().setAppName("Testing").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val itemdata = sc.textFile("data/item.csv")
    val itemDF =itemdata.map(line=>line.split(",")).map(row =>Item1(row(0),row(1),row(2).toInt,row(3))).toDS()
    itemDF.toDF().registerTempTable("items")

    val companydata=sc.textFile("data/company.csv")
    val companyDF =companydata.map(line=>line.split(",")).map(row =>Company1(row(0),row(1),row(2))).toDS()
    sc.broadcast(companyDF)
    companyDF.count()
    companyDF.toDF().registerTempTable("companies")

    val result = sqlContext.sql("SELECT * FROM companies C JOIN items I ON C.companyId= I.companyId")
    result.show()
//16/08/22 09:44:35 INFO BlockManagerInfo: Added broadcast_5_piece0 in memory on 10.43.38.241:52458 (size: 6.9 KB, free: 1068.3 MB)
  }

}
