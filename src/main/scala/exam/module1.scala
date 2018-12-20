package exam

import org.apache.spark.sql.SparkSession

/**
  * Created by satyak on 9/11/2016.
  */
object module1 {
  def main(args: Array[String])={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    val spark = SparkSession
      .builder
      .master("local")
      .appName("Module1")
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // this is used to implicitly convert an RDD to a DataFrame.
    import spark.implicits._

    val data =sqlContext.read.json("/user/data/task2")
    data.toDF().registerTempTable("dfdatatbl")
    val finaldata= spark.sql("select state,avg(cast (balance as double)) from dfdatatbl where balance>2000 group by state order by state asc")
    finaldata.show()
    finaldata.rdd.map(line=>line.mkString("\t")).repartition(1).saveAsTextFile("/user/data/results/task2")
  }

}
