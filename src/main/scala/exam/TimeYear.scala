package exam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

object TimeYear {
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
        import sqlContext.implicits._
        val dataDF =sc.textFile("data/time").toDF("ts")
        dataDF.printSchema()
        dataDF.registerTempTable("df1")
        sqlContext.sql("select ts ,substring(ts,0,4) from df1  where substring(ts,0,4) ='2015'").show()
  }
  
}  