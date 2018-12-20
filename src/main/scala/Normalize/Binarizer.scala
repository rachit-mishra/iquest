package Normalize

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
//core and SparkSQL
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.DataFrame
// ML Feature Creation, Tuning, Models, and Model Evaluation
import org.apache.spark.ml.feature.{ StringIndexer, VectorAssembler, OneHotEncoder }
import org.apache.spark.ml.tuning.{ ParamGridBuilder, TrainValidationSplit }
import org.apache.spark.ml.evaluation.{ RegressionEvaluator }
import org.apache.spark.ml.regression.{ LinearRegression }
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics
import com.github.fommil.netlib._

/**
 * Created by Ravee on 2/20/2017.
 */
object Binarizer {

  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
      .getOrCreate()

    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame = spark.createDataFrame(data).toDF("id", "feature")

    import org.apache.spark.ml.feature.Binarizer
    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.5)

    val binarizedDataFrame = binarizer.transform(dataFrame)

    println(s"Binarizer output with Threshold = ${binarizer.getThreshold}")
    binarizedDataFrame.show()

  }

}