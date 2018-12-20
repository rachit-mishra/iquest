package exam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}


object DataTyeConversion2 {

  case class Longs(org_id: String, latitude: String, longitude: String)

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
      val salesemp = sc.textFile("data/lats.txt")

      import sqlContext.implicits._
      val DF1 = salesemp.map(_.split(",")).map( row => Longs(row(0), row(1), row(2))).toDF()
      DF1.registerTempTable("df1tbl")
      val  logop_grid_DF = sqlContext.sql("SELECT org_id, latitude, longitude  from df1tbl where cast(latitude as double) BETWEEN -80 and 80 ")

      logop_grid_DF.printSchema()
      logop_grid_DF.show()

}
  
}  