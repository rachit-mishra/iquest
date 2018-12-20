package exam

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by satyak on 8/20/2016.
  */
object modul2_accumulator {

  def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

    //spark 2
    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val nErrors=sc.accumulator(0.0)
    val logs = sc.textFile("/user/data2/output.log")
    logs.filter(line =>line.contains("ERROR")).foreach(x=>nErrors+=1)
    val vnt = nErrors.value

    println("======================>>>>Count : >>>"+vnt)


  }


}
