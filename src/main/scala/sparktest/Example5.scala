package sparktest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by ravi on 6/26/17.
  */
object Example5 {

  case class ExchangeData(dollar:Float, euro:Float, year:String)

  def main(args:Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("Exam5").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val data = sc.textFile("/Users/ravi/training/Certification/data/task5.txt")
                  .map(s => s.split(","))
                    .map(row => ExchangeData(row(0).toFloat, row(1).toFloat, row(2) ) ).toDF()




    data.registerTempTable("exchange_data")

    val output = sqlContext.sql(" select avg(dollar), avg(euro) from exchange_data where substring(year,0,4)='2016' ")

    output.show()

    output.rdd.map(s => s.mkString(",")).repartition(1).saveAsTextFile("/Users/ravi/training/Certification/output/task5")
  }
}
