package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}

object RankAxiom {

  def main(args: Array[String]) ={
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

  //spark 2
  val sparkSession = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .getOrCreate()
val dataDF = sparkSession.read.json("aff.json")
import sparkSession.implicits._
dataDF.registerTempTable("arff_file")

sparkSession.sql("""select  rank() over (partition by acxiom_key order by  ninety_dq desc, mondiff desc) rnk,record_nb, acxiom_key, mondiff, sec_type, rank_tier1, tradetype,  app_cg,ninety_dq, uns_type  ,
trade_payment from arff_file where sec_type = 1""").show()

}
  
}  