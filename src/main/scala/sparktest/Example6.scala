package sparktest

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ravi on 6/26/17.
  */
object Example6 {

  case class Station(id:String, name:String)
  case class Weather(id:String, altitude:Int, windspeed:Int, temperature:Float)

  def main (args:Array[String]) : Unit = {

    val conf = new SparkConf().setAppName("Exam6").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._


    val stations = sc.textFile("/Users/ravi/training/Certification/data/task6-station.txt")
                      .map(s => s.split(","))
                      .map(row => Station(row(0),row(1))).toDF().registerTempTable("stations")

    val weather = sc.textFile("/Users/ravi/training/Certification/data/task6-weather.txt")
                     .map(s => s.split(","))
                     .map(row => Weather(row(0),row(1).toInt, row(2).toInt, row(3).toFloat )).toDF().registerTempTable("weather")

    val output = sqlContext.sql("select s.id, s.name, w.altitude, w.windspeed, w.temperature from stations s, " +
      "weather w where s.id = w.id order by w.temperature asc ")

    output.show()

    output.rdd.map(s=>s.mkString(",")).repartition(1).saveAsTextFile("/Users/ravi/training/Certification/output/task6")


  }
}
