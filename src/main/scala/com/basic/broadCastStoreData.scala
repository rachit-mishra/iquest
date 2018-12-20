package com.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ravee on 8/20/2016.
  */
object broadCastStoreData {


  def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

    val conf = new SparkConf().setAppName("Testing").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val localDF = sqlContext.read.json("us_states.json")
   // localDF.show()
    val broadcastStateData = sc.broadcast(localDF.collectAsList())
    val broadcastSchema = sc.broadcast(localDF.schema)

    // Create a DataFrame of US state data with the broadcast variables.
    val stateDF = sqlContext.createDataFrame(broadcastStateData.value, broadcastSchema.value)
    //stateDF.show()
    stateDF.registerTempTable("stateDF1")
    //
    val storesDF = sqlContext.read.json("store_locations.json")

   // println("How many stores are in each US region?")
    //val joinedDF = storesDF.join(stateDF, "state").groupBy("census_region").count()
    //joinedDF.show()

    storesDF.registerTempTable("storesDF1")

    val joinedDF1 =sqlContext.sql("select a.state, b.city from stateDF1 a ,storesDF1 b where a.state=b.state ")
    joinedDF1.show()

    //This shows spark is using broad cast variables
    // 16/08/22 09:26:19 INFO BlockManagerInfo: Added broadcast_8_piece0 in memory on 10.43.38.241:49332 (size: 18.7 KB, free: 1068.3 MB)


  }

}
