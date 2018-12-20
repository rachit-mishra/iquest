package Normalize

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
//core and SparkSQL
import org.apache.spark.{SparkConf, SparkContext}
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
object HashTF {


  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
      .getOrCreate()

    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer,CountVectorizer,StandardScaler}

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val tokenizerwordsData = tokenizer.transform(sentenceData)
    tokenizerwordsData.show(false)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(tokenizerwordsData)
    featurizedData.show(false)

    // alternatively, CountVectorizer can also be used to get term frequency vectors
    val CountVectorizer = new  CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(tokenizerwordsData)
    CountVectorizer.transform(tokenizerwordsData).show(false)




  }

}