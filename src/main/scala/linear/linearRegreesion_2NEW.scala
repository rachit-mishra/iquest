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
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.regression.LinearRegressionSummary
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger}
import com.github.fommil.netlib.BLAS;


object linearRegreesion_2NEW {
  
  //
  val stateHolidayIndexer = new StringIndexer().setInputCol("StateHoliday").setOutputCol("StateHolidayIndex")
  val stateHolidayEncoder = new OneHotEncoder().setInputCol("StateHolidayIndex") .setOutputCol("StateHolidayVec")

  val schoolHolidayIndexer = new StringIndexer().setInputCol("SchoolHoliday").setOutputCol("SchoolHolidayIndex")
   val schoolHolidayEncoder = new OneHotEncoder().setInputCol("SchoolHolidayIndex") .setOutputCol("SchoolHolidayVec")

  val dayOfMonthEncoder = new OneHotEncoder().setInputCol("DayOfMonth").setOutputCol("DayOfMonthVec")
  val dayOfWeekEncoder = new OneHotEncoder().setInputCol("DayOfWeek").setOutputCol("DayOfWeekVec")

   val storeEncoder = new OneHotEncoder().setInputCol("Store").setOutputCol("StoreVec")

  //assembler
  val assembler = new VectorAssembler()  .setInputCols(Array("StoreVec", "DayOfWeekVec", "Open",    "DayOfMonthVec", "StateHolidayVec", "SchoolHolidayVec"))  .setOutputCol("features")
  val logger = Logger.getLogger(getClass.getName)
 
def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
  
     val spark = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .config("spark.some.config.option", "config-value")
                          .getOrCreate()
                         
  import spark.implicits._
  val data  = spark
    .read.format("com.databricks.spark.csv")
    .option("header", "true")
    .load("train.csv")
    .repartition(6)
  data.registerTempTable("raw_training_data")

  val data2=spark.sql("""SELECT
        CAST (Sales as DOUBLE) label,  CAST (Store as DOUBLE) Store,  CAST (Open as DOUBLE),
        CAST (DayOfWeek as DOUBLE),
        StateHoliday, SchoolHoliday,  CAST (regexp_extract(Date, '\\d+-\\d+-(\\d+)', 1) as DOUBLE) DayOfMonth
        FROM raw_training_data""").na.drop()

  val lr = new LinearRegression()
  println(lr.explainParams())

  val paramGrid = new ParamGridBuilder()
    .addGrid(lr.regParam, Array(0.1, 0.01))
    .addGrid(lr.fitIntercept)
    .addGrid(lr.elasticNetParam, Array(0.0, 0.25, 0.5, 0.75, 1.0))
    .build()
  val trainingData = assembler.transform(data2)
  val lrModel1 = lr.fit(trainingData)
  val trainingSummary = lrModel1.summary
  trainingSummary.residuals.show()

  println(lrModel1.coefficients)
  println(lrModel1.intercept)
  println(lrModel1.summary.predictionCol)

  val eval = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("label")
    .setMetricName("rmse")

  //////
  val pipeline = new Pipeline()
    .setStages(Array(stateHolidayIndexer, schoolHolidayIndexer,
      stateHolidayEncoder, schoolHolidayEncoder, storeEncoder,
      dayOfWeekEncoder, dayOfMonthEncoder,
      assembler, lr)
    )

  val Array(training, test) = data2.randomSplit(Array(0.8, 0.2), seed = 12345)
  logger.info("Fitting data")
  val model = pipeline.fit(training)

  logger.info("Now performing test")
  val holdout = model.transform(test)  //.select("prediction","label")

   eval.evaluate(holdout)


  holdout.selectExpr("label", "prediction", "label - prediction Residual_Error",
    "(label - prediction) /{} Within_RSME".format("rmse"))
    .registerTempTable("RMSE_Evaluation")

  // have to do a type conversion for RegressionMetrics
  val rm = new RegressionMetrics(holdout.rdd.map(x =>
    (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))


  logger.info("Test Metrics")
  logger.info("Test Explained Variance:")
  logger.info(rm.explainedVariance)
  logger.info("Test R^2 Coef:")
  logger.info(rm.r2)
  logger.info("Test MSE:")
  logger.info(rm.meanSquaredError)
  logger.info("Test RMSE:")
  logger.info(rm.rootMeanSquaredError)


}



}