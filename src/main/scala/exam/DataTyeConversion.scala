package exam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}


object DataTyeConversion {

  case class Employee(dep: String, des: String, cost: String, state: String)

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
      val DF1 = salesemp.map(_.split(",")).map( row => Employee(row(0), row(1), row(2),row(3))).toDF()
      DF1.registerTempTable("df1tbl")
      val df2 = sqlContext.sql("select dep,des,cast (cost as double), state from df1tbl")
      df2.show()
      df2.printSchema()

}
  
}  