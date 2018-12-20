package exam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}


object sales5 {

  case class Employee(dep: String, des: String, cost: Double, state: String)
  case class Employee2(dep: String, des: String, cost: Double)

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
      val salesemp = sc.textFile("data/salex.txt")

      import sqlContext.implicits._
      val DF1 = salesemp.map(_.split(",")).map( row => Employee2(row(0), row(1), row(2).toDouble)).toDF()

      DF1.registerTempTable("salesTbl")
      val dataDF = spark.sql("select * from slaesTbl")
      dataDF.show()


}
  
}  