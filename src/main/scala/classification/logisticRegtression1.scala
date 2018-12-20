package classification

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ StructType, StructField, StringType }
import org.apache.spark.sql.types.{ StructType, StructField, LongType }
import org.apache.spark.sql.types.{ StructType, StructField, IntegerType }
import org.apache.spark.sql.types.{ StructType, StructField, DoubleType }
import org.apache.spark.ml.feature.{ StringIndexer, VectorAssembler, OneHotEncoder }
import org.apache.spark.ml.tuning.{ ParamGridBuilder, TrainValidationSplit }
import org.apache.spark.ml.evaluation.{ RegressionEvaluator }
import org.apache.spark.ml.regression.{ LinearRegression }
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.log4j.{ Logger }
import com.github.fommil.netlib.BLAS;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.ml.feature.{ HashingTF, IDF, Tokenizer }
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.classification.{ LogisticRegression }

object logisticRegtression1 {

  val logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()

    val sqlContext = new org.apache.spark.sql.SQLContext(sparkSession.sparkContext)

    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat"))).toDF("Label", "Text")

    val numFeatures = 64000
    val numEpochs = 30
    val regParam = 0.02

    val tokenizer = new Tokenizer().setInputCol("Text").setOutputCol("Words")
    val hashingTF = new org.apache.spark.ml.feature.HashingTF()
      .setNumFeatures(numFeatures).
      setInputCol(tokenizer.getOutputCol).setOutputCol("Features")
      
    val lr = new LogisticRegression()
      .setMaxIter(numEpochs)
      .setRegParam(regParam)
      .setFeaturesCol("Features")
      .setLabelCol("Label").
      setRawPredictionCol("Score").setPredictionCol("Prediction")
    
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

    val model = pipeline.fit(sentenceData)
    val result1 = model.transform(sentenceData)
    result1.show()

    val testTitle = "Easiest way to merge a release into one JAR file"
    val testBody = """Is there a tool or script which easily merges a bunch of 
                   href="http://en.wikipedia.org/wiki/JAR_%28file_format%29"
                   JAR files into one JAR file? A bonus would be to easily set the main-file manifest 
                   and make it executable. I would like to run it with something like:
                  As far as I can tell, it has no dependencies which indicates that it shouldn't be an easy 
                  single-file tool, but the downloaded ZIP file contains a lot of libraries."""
    
    val testText = testTitle + testBody
    //val testText = "Trump and Hillary having fun on Americaan folsk"
    val testDF = sqlContext.createDataFrame(Seq((200320304, testText)))
      .toDF("Label", "Text")
    val result = model.transform(testDF)
    result.show()
    val prediction = result.collect()(0)(6).asInstanceOf[Double]
    print("Prediction: " + prediction)

  }
}