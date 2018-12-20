package recomandation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.Predictor
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object movieRecommend {
  
//case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
case class Rating(userId: Int, movieId: Int, rating: Float)

  object Rating {
    def parseRating(str: String): Rating = {
      val fields = str.split("::")
      assert(fields.size == 4)
      //Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
    }
  }

  def main(args: Array[String]) {
      System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
      Logger.getLogger("org").setLevel(Level.WARN)
     val sparkSession = SparkSession.builder
                 .master("local")
                 .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
                 .appName("my-spark-app")
                 .getOrCreate()
                          
    val sc =  sparkSession.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkSession.sparkContext)

    import sqlContext.implicits._
    val ratings = sc.textFile("data/ratings.dat.txt")
      .map(Rating.parseRating)
      .toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    val numTraining = training.count
    val numTest = test.count
    println("Training: " + numTraining + ", test: " + numTest)

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    val predictions = model.transform(test)
      .withColumn("rating", col("rating").cast(DoubleType))
      .withColumn("prediction", col("prediction").cast(DoubleType))

    predictions.printSchema()
    predictions.show()

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
    sc.stop()


  }
}