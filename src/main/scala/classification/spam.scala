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
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.types.{StructType, StructField, LongType}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}
import org.apache.spark.sql.types.{StructType, StructField, DoubleType}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.regression.{LinearRegression}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger}
import com.github.fommil.netlib.BLAS;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint

object spam {
  
  val logger = Logger.getLogger(getClass.getName)
 
def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
  
     val sparkSession = SparkSession.builder
       .master("local")
       .appName("my-spark-app")
       .config("spark.some.config.option", "config-value")
       .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
       .getOrCreate()
                         
  //sql context                        
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkSession.sparkContext)
  import sqlContext.implicits._
  
    // Load 2 types of emails from text files: spam and ham (non-spam).
    // Each line has text from one email.
    val spam = sparkSession.read.text("C:\\Users\\Ravee\\Downloads\\spam\\spam\\*.txt")
    val ham = sparkSession.read.text("C:\\Users\\Ravee\\Downloads\\ham\\ham\\*.txt")

    //(1)Feature extraction
    // Create a HashingTF instance to map email text to vectors of 100 features.
    val tf = new HashingTF(numFeatures = 10000)
    
    // Each email is split into words, and each word is mapped to one feature.
   //val spamFeatures = spam.rdd.map(email => tf.transform(email.split(" ")))
//    val hamFeatures = ham.rdd.map(email => tf.transform(email.split(" ")))

    //(2)Create Training Set
    // Create LabeledPoint datasets for positive (spam) and negative (ham) examples.
    //val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    //val negativeExamples = hamFeatures.map(features => LabeledPoint(0, features))
    //val trainingData = positiveExamples ++ negativeExamples
    
    //val trainingData = positiveExamples.union(negativeExamples)
    //trainingData.cache() // Cache data since Logistic Regression is an iterative algorithm.

    //(3) Training
    // Create a Logistic Regression learner which uses the LBFGS optimizer.
    val lrLearner = new LogisticRegressionWithSGD()
    // Run the actual learning algorithm on the training data.
   // val model = lrLearner.run(trainingData)

    //(4) Create Testing Set
    // Test on a positive example (spam) and a negative one (ham).
    // First apply the same HashingTF feature transformation used on the training data.
    val posTestExample = tf.transform("insurance plan which change your life ...".split(" "))
    val negTestExample = tf.transform("hi sorry yaar i forget tell you i cant come today".split(" "))

    //(5) Test results
    // Now use the learned model to predict spam/ham for new emails.
   // println(s"Prediction for positive test example: ${model.predict(posTestExample)}")
    //println(s"Prediction for negative test example: ${model.predict(negTestExample)}")
    
    val sOne = "iPhone Leather Sleeve CASSIOPEIA http://t.co/EMtxZNU2ro | " +
                        "#iphone #iphone5 #iPhone5Sleeve #iPhoneSleeve " +
                        "#iPhone5sSleeve #iPhone5s #Swarovski"
        val sTwo = "this is non spam text "
        val sThree = "@airtelghana  thank you soo much #iphone5s​"
        val posTest = tf.transform(sOne.split(" "))
        val negTest = tf.transform(sTwo.split(" "))
        val thirdTest = tf.transform(sThree.split(" "))
       // println("Prediction for Spam '" + sOne  + "' : "+ model.predict(posTest))
       // println("Prediction for spam '" + sTwo + "' : " + model.predict(negTest))
       // println("Prediction for spam '" + sThree + "' :  " + model.predict(thirdTest))
        
        

  }
}