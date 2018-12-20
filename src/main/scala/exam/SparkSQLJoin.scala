package exam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by satyak on 8/20/2016.
  */
object SparkSQLJoin {

  case class Item(id:String, name:String, unit:Int, companyId:String)
  case class Company(companyId:String, name:String, city:String)

  def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

    val conf = new SparkConf().setAppName("Testing").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val i1 = Item("1", "first", 1, "c1")
    val i2 = Item("2", "second", 2, "c2")
    val i3 = Item("3", "third", 3, "c3")
    val c1 = Company("c1", "company-1", "city-1")
    val c2 = Company("c2", "company-2", "city-2")

    import sqlContext.implicits._
    val companies = sc.parallelize(List(c1,c2))
    companies.toDF().registerTempTable("companies")

    val items = sc.parallelize(List(i1,i2,i3))
    items.toDF().registerTempTable("items")
    val result = sqlContext.sql("SELECT * FROM companies C JOIN items I ON C.companyId= I.companyId")
    result.show()

 }

}
