package exam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}


object task2_readJSONSCALA{

     def main(args: Array[String]) ={

      System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
        //sparEXAMDATAk 2
        val spark = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .config("spark.some.config.option", "config-value")
                          .getOrCreate()

        val sc = spark.sparkContext
        val ksn = new org.apache.spark.sql.SQLContext(sc)


        import ksn.implicits._
        val dataDF =spark.read.json("/user/ubuntu/error.json")

        //dataDF.show()
        dataDF.registerTempTable("df1")
        val df2= ksn.sql("select count(*) from df1")
       //filter , average, group by
        df2.printSchema()
        // df2.show()

}
  
}  