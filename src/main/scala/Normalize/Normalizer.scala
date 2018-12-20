package Normalize

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
//core and SparkSQL
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
// ML Feature Creation, Tuning, Models, and Model Evaluation
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.regression.{LinearRegression}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics
import com.github.fommil.netlib._


/**
  * Created by Ravee on 2/20/2017.
  */
object Normalizer {


  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.ml.feature.Normalizer
    import org.apache.spark.ml.linalg.Vectors

    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.5, -1.0)),
      (1, Vectors.dense(2.0, 1.0, 1.0)),
      (2, Vectors.dense(4.0, 10.0, 2.0))
    )).toDF("id", "features")

    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val l1NormData = normalizer.transform(dataFrame)
    println("Normalized using L^1 norm")
    l1NormData.show()
    l1NormData.printSchema()

  }

}