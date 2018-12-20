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

object AmazonReviewsNEW {

  val logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")

      .getOrCreate()

    val sc = sparkSession.sparkContext

    val tok = new Tokenizer().setInputCol("review").setOutputCol("words")
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("features").setNumFeatures(2)
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
    val pipeline = new Pipeline().setStages(Array(tok, hashingTF, lr))
    val training = sparkSession.read.json("amazonratings.json")
    val model = pipeline.fit(training)
    val Modelprediction = model.transform(training)
    Modelprediction.show()

    //Predict it
    val testdf = sparkSession.read.json("amazonratingsTEST.json")
    val prediction = model.transform(testdf)
    prediction.show()
    //val selected = prediction.select("words", "prediction")  
    // selected.show()

  }
}