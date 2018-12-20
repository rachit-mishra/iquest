package sparktest

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ravi on 6/26/17.
  */
object Example1 {

  case class Flight(country: String, year: String, day: String, month: String, airport: String, departure: Int, arrival: Int)

  def main(args:Array[String]) =
  {

    val conf = new SparkConf().setAppName("Exam1").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val data = sc.textFile("/Users/ravi/training/Certification/data/task1.txt")
              .map(s => s.split(","))
              .map(row => Flight( row(0), row(1), row(2), row(3), row(4), row(5).toInt, row(6).toInt ) )
              .toDF()



    data.registerTempTable("flights")

    var output = sqlContext.sql("select * from flights where departure >= 10 order by departure asc ")

    output.show()

    output.rdd.repartition(1).saveAsTextFile("/Users/ravi/training/Certification/output/task1")
  }
}
