package sparktest

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ravi on 6/26/17.
  */
object Example2 {
  def main (arg:Array[String]) : Unit = {

    val conf = new SparkConf().setAppName("Exam2").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);

    val data = sqlContext.read.json("/Users/ravi/training/Certification/data/task2.json")

    data.registerTempTable("state_balance")

    val output = sqlContext.sql("select state, avg(balance) from state_balance where balance > 2000 group by state")


    output.show()

    output.rdd.repartition(1).saveAsTextFile("/Users/ravi/training/Certification/output/task2")

  }
}
