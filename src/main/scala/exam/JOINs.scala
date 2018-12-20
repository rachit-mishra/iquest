package exam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}


object JOINs {

  case class sim (codeA:String,name:String )
  case class event (codeA:String,value:String )
    def main(args: Array[String]) ={

      System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

      val spark = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .config("spark.some.config.option", "config-value")
                          .getOrCreate()
      val sc = spark.sparkContext
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._

      val data1 = sc.textFile("data/sim.txt")
      val df1= data1.map(line =>line.split(",")).map(row =>sim(row(0),row(1))).toDS()
      df1.toDF().registerTempTable("df1t")

      val data2 = sc.textFile("data/events.txt")
      val df2= data2.map(line =>line.split(",")).map(row =>event(row(0),row(1))).toDS()
      df2.registerTempTable("df2t")

      val joindf= sqlContext.sql("SELECT concat('1',a.codeA),a.value from df2t a, df1t b where a.codeA=b.codeA").toDF()
      joindf.show()
  }

}  