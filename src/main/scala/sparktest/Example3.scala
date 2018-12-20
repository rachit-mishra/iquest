package sparktest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ravi on 6/26/17.
  */
object Example3 {
  def main (args:Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("Exam3").setMaster("local")
    val sc = new SparkContext(conf)

    val nErrors=sc.accumulator(0)

    val data = sc.textFile("/Users/ravi/training/Certification/data/task3.txt")
                  .filter(s => s.contains("ERROR")).foreach(x=>nErrors+=1)

    println("There are " + nErrors + " ERROR Statements " )


  }
}
