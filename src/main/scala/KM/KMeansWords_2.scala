package KM

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession

object KMeansWords_2 {
  def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
  
     val sparkSession = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .config("spark.some.config.option", "config-value")
       .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
       .getOrCreate()
                          
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkSession.sparkContext)
  val sentenceData = sqlContext.createDataFrame(Seq(
                              ("Hi I heard about Spark",1),
                              ("I wish Java could use case classes",1),
                              ("K-means models are neat",1)
                         )).toDF("sentence","id")

 // initialize pipeline stages
 val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
 val hashingTF = new HashingTF().setInputCol("words").setOutputCol("features").setNumFeatures(20)
 val kmeans = new KMeans()
 val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, kmeans))

 // fit the pipeline
 val fitKmeans = pipeline.fit(sentenceData)
 val centers = fitKmeans.stages(2).asInstanceOf[KMeansModel].clusterCenters
 val predictionResult = fitKmeans.transform(sentenceData)
  predictionResult.show()

    //create your test data frame to predict cluster of your data words
    val sentenceData1 = sqlContext.createDataFrame(Seq(
      ("Where is ambariCloud fit",999)
    )).toDF("sentence","id")

    val predictionResult1 = fitKmeans.transform(sentenceData1)
    predictionResult1.show()

 }



}