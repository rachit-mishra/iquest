package sparktest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Example4 {

  case class Weather(location: String, year: Int, month: Int, day: Int, inches: String)

  def main (args:Array[String]) : Unit = {

    val conf = new SparkConf().setAppName("Exam4").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val data = sc.textFile("/Users/ravi/training/Certification/data/task4.txt")
                    .map(s => s.split(","))
                    .map(row => Weather(row(0), row(1).toInt, row(2).toInt, row(3).toInt, row(4) ) ).toDF()

    data.registerTempTable("weather")

    val output = sqlContext.sql(" select * from weather where cast(inches as int) > 10 ")

    output.show()

    output.rdd.map(row => row.mkString(",")).repartition(1).saveAsTextFile("/Users/ravi/training/Certification/output/task4")

  }
}


