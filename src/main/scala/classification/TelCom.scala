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
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.mllib.classification.NaiveBayes  
import org.apache.spark.mllib.regression.LabeledPoint  
import org.apache.spark.{SparkContext, SparkConf}  
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.classification.{LogisticRegression,RandomForestClassifier}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

object TelCom {
  def main(args:Array[String])    {

  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
  val spark = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .config("spark.some.config.option", "config-value")
    .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")

    .getOrCreate()

  val label_indexer  = new StringIndexer().setInputCol("churned").setOutputCol("label")
  val plan_indexer  = new StringIndexer().setInputCol("intl_plan").setOutputCol("intl_plan_indexed")

  val assembler = new VectorAssembler()  .setInputCols(Array("intl_plan_indexed", "account_length",
    "number_vmail_messages", "total_day_calls","total_day_charge", "total_eve_calls",
    "totl_eve_charge","total_night_calls", "total_intl_calls", "total_intl_charge"))
    .setOutputCol("Features")
   
  //train data
  val train =spark.read.format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("/data/Telcom.txt")
 // train.printSchema()
 // train.show()
  
  //test data
  val test =spark.read.format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("/data/TelcomTest.txt")

  val classifier = new RandomForestClassifier().setFeaturesCol("Features").setLabelCol("label")
  val pipeline = new Pipeline().setStages(Array(plan_indexer, label_indexer, assembler, classifier))
  val model = pipeline.fit(train)
  
 val predictions = model.transform(test)
    predictions.show()
 val result = predictions.select("prediction","label").rdd
 
 val predictionAndLabels = result.map { row =>
      (row.get(0).asInstanceOf[Double],row.get(1).asInstanceOf[Double])
    } 
        
  val metrics = new BinaryClassificationMetrics(predictionAndLabels)
  println("Area under ROC = " + metrics.areaUnderROC())
  //0.8, indicating that the model’s results are reasonably good, and definitely better than random guessing.
  
  }
}