package com.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ravee on 8/27/2016.
  */
object JSOnToDS {

  case class Errors22(rowkey: String, invldpg:String, userid:String)

  def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")


    val conf = new SparkConf().setAppName("Testing").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name.
    val path = "data/error.json"
    val people = sqlContext.read.json(path).as[Errors22]
    people.show()

  }

}
