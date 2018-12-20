package exam

import com.basic.broadCast
import broadCast.{Company, Item}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by satyak on 8/28/2016.
  */
object module3_filterExample {

  case class weather(stationid: String,
                     elevation:String, windspeed:String, temp:String)

  def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

    val conf = new SparkConf().setAppName("Testing").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val weaterdata =sc.textFile("data/weather.txt")

   val data1= weaterdata.map(line =>line.split(",")).filter(row =>row(3)!="****").filter(r=>r(3).toInt>=32)
    val df =data1.map(row =>weather(row(0),row(1),row(2),row(3))).toDF()
    df.printSchema()
    df.registerTempTable("weatherTable")
    sqlContext.sql("select stationid,elevation,windspeed,temp " +
      "from weatherTable where stationid='125x'" +
      "order by temp asc").show()
  }
}

