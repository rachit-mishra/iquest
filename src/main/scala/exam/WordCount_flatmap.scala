package exam

import org.apache.spark.{SparkConf, SparkContext}
    
//standared word count map reduce using old style conf    
object WordCount_flatmap {
  def main(args: Array[String]) ={
     
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
      
    //config and context
    val conf = new SparkConf().setAppName("Testing").setMaster("local")
    val sc = new SparkContext(conf)
    //read file as RDD
    val test = sc.textFile("data/data.txt")
    //convert to flat map
    val flat=  test.flatMap { line  => line.split(" ") }
    //
    flat.map { word => (word,1) }
      .reduceByKey(_ + _)
      .saveAsTextFile("/user/data/count")
   
    }
  
}