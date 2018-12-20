package exam

import org.apache.spark.{SparkConf, SparkContext}

object RemoveHead {
  
  case class Emp(dep: String, des: String, cost: String)
    
  def main(args: Array[String]): Unit = {
    
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    //spark 2
    val conf = new SparkConf().setAppName("Testing").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // Read the CSV file
    val csv = sc.textFile("data/files.txt")
    // val first = csv.first()
    val df22 =csv.filter { x => x!=csv.first() }
    import sqlContext.implicits._
    val salesempDF = df22.map(_.split(",")).map( row => Emp(row(0), row(1), row(2) )).toDF()
    
    //all rows including header
    salesempDF.show()
    //////////////////////////////////////////////////////////
    //get header row
   // val headerDF = salesempDF.first()
    //filter header row
   // val finalDF =salesempDF.filter(row =>row!=headerDF) 
    //finalDF.show()
    ////////////////////////////////////////////////////////////////////////

  }
}