package exam

import org.apache.spark.sql.SparkSession

object SaveMethods {
  case class Employee(dep: String, des: String, cost: Double, state: String)
  def main(args: Array[String]) ={
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    //spark 2
    val spark = SparkSession.builder
                          .master("local")
                          .appName("saveMethod")
                          .config("spark.sql.warehouse.dir","file:///C:/Users/satyak/IdeaProjects/Testing/spark-warehous")
                          .getOrCreate()
    //1.6
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val salesemp = sc.textFile("data/salex.txt")
    import sqlContext.implicits._

    val salesempDF = salesemp.map(_.split(",")).map( row => Employee(row(0), row(1), row(2).toDouble, row(3) )).toDF()
    salesempDF.registerTempTable("salesempDFTbl")
    val results = spark.sql("select * from salesempDFTbl gr")

    //store data
    //this adds brackets for each line
    results.rdd.saveAsTextFile("/user/data/sales1")
    //csv
    results.rdd.map(line =>line.mkString(",")).saveAsTextFile("/user/data/sales2")
    //tab
    results.rdd.map(line =>line.mkString("\t")).saveAsTextFile("/user/data/sales3")
    //add headers
    results.write.format("com.databricks.spark.csv")
      .option("header","true").save("/user/data/sales5")

    //Home work ; try these
    results.rdd.coalesce(1).map(line =>line.mkString("\t")).saveAsTextFile("/user/data/sales4")
    results.toJSON.coalesce(1).write.save("/user/data/sales6")
    results.coalesce(1).write.mode("append").json("/user/data/sales7")

    results.write.parquet("employee.parquet")
    //repartition vs coalesce
 }
}  