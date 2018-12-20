package linear

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


object StoreLiner_4 {
  

  val assembler = new VectorAssembler()  .setInputCols(Array("val2","val3","val4"))  .setOutputCol("features")
  val logger = Logger.getLogger(getClass.getName)
 
def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
  
     val spark = SparkSession.builder
                          .master("local")
                          .appName("Store")
                          .config("spark.some.config.option", "config-value")
                          .getOrCreate()
  import spark.implicits._

  //get data
  val data = loadTrainingData(spark)
  // Train Model
  val linearTvs = preppedLRPipeline()
  val model = linearTvs.fit(data)
  
  //display Model results
  val holdout = model.transform(data).select("prediction","label")
  holdout.show()
  holdout.printSchema()
  
  //Test the mode
  val testdataDF= spark.read.json("liner2Predict.json")
    .repartition(6).na.drop()

  val testholdout = model.transform(testdataDF)
    .select("prediction","label")

  testholdout.show()
}
  def preppedLRPipeline():Pipeline = {
  val lr = new LinearRegression()
  val pipeline = new Pipeline()    .setStages(Array(assembler,lr))
  pipeline
}

  def loadTrainingData(Sparksession:SparkSession):DataFrame = {
    Sparksession.read.json("liner2.json")
    .repartition(6).na.drop()
 }

  def fitModel(tvs:Pipeline, data:DataFrame) = {
    val Array(training, test) = data.randomSplit(Array(0.8, 0.2), seed = 12345)
    logger.info("Fitting data")
    val model = tvs.fit(training)
    logger.info("Now performing test on hold out set")
    val holdout = model.transform(test).select("prediction","label")

    // have to do a type conversion for RegressionMetrics
    val rm = new RegressionMetrics(holdout.rdd.map(x =>
    (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

  logger.info("Test Metrics")
  logger.info("Test Explained Variance:")
  logger.info(rm.explainedVariance)
  logger.info("Test R^2 Coef:")
  logger.info(rm.r2)
  logger.info("Test MSE: Cost Function ")
  logger.info(rm.meanSquaredError)
  logger.info("Test RMSE:")
  logger.info(rm.rootMeanSquaredError)
    model
  }

}