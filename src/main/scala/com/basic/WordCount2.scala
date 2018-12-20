package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
    import org.apache.spark.sql.SparkSession
    

    //workd count using spark session
object WordCount2 {
  def main(args: Array[String]) ={
     
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
      val sparkSession = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .config("spark.some.config.option", "config-value")
                          .getOrCreate()
  
    //read file
    val test = sparkSession.read.text("data.txt")
    //register as table and get count
    test.registerTempTable("test")
    val count = sparkSession.sql("select count(*) as cnt from test")
    //save count as json
    count.write.format("json")save("count2")
    
    
  }
  
}