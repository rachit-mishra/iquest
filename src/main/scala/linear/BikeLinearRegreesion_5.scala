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
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import com.github.fommil.netlib.BLAS;

case class bike (instant:Double,dteday:String,season:Double,yr:Double,mnth:Double,hr:Double,holiday:Double,weekday:Double, workingday:Double,weathersit:Double,
                 temp:Double,atemp:Double,hum:Double,windspeed:Double,casual:Double,registered:Double,label:Double)

object BikeLinearRegression_5 {
  val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) ={
      System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
  
     val spark = SparkSession.builder
       .master("local")
       .appName("my-spark-app")
       .config("spark.some.config.option", "config-value")
       .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
       .getOrCreate()

  import spark.implicits._
  //Read Training data
  val data = loadTrainingData(spark)

  //build pipeline model
  val ppl = LRPipeline

  //Train the model
  val model = ppl.fit(data)
  val trainingPredict = model.transform(data)
                      .withColumnRenamed("prediction","Predictcnt")
                      .withColumnRenamed("label","Inputcnt")
                      .withColumnRenamed("instant","instantID")
                      .select( "Predictcnt","Inputcnt","instant")
  trainingPredict.show()
  trainingPredict.printSchema()

  val rm = new RegressionMetrics(
      trainingPredict.rdd
        .map(x =>( x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])
        )           )
    logger.info("Test Metrics----------------->")
    logger.info("Test Explained Variance:>>>>>>>>>>>>>>>>>>"+rm.explainedVariance)
    logger.info("Test R^2 Coef:>>>>>>>>>>>>>>>>>>>>>>"+rm.r2)
    logger.info("Test MSE: COST FUNCTION===========>"+rm.meanSquaredError)
    logger.info("Test RMSE:->>>>>>>>>>>>>>>>>"+rm.rootMeanSquaredError)

  //Predict using TEST data
  val dfPredict = loadTestgData(spark)
  val predict = model.transform(dfPredict)
                      .withColumnRenamed("prediction","Predictcnt")
                      .withColumnRenamed("label","Inputcnt")
                      .withColumnRenamed("instant","instantID")
                      .select("instant", "Predictcnt","Inputcnt")

    println("==================Testing the model ========================")
    predict.show()
  }

  def LRPipeline():Pipeline = {
    val lr = new LinearRegression()
    lr.setMaxIter(10)
    lr.setRegParam(0.01)

    val assembler = new VectorAssembler().setInputCols(Array("weekday","workingday","holiday","weathersit","temp","atemp","hum","windspeed")).setOutputCol("features")
    import org.apache.spark.ml.feature.StandardScaler
//    val scaler = new StandardScaler().setInputCol("rawfeatures").setOutputCol("features").setWithStd(true).setWithMean(true)

    val pipeline = new Pipeline().setStages(Array(assembler,lr))
    return pipeline
  }
  
  def loadTrainingData(Sparksession:SparkSession):DataFrame= {
    val df = Sparksession.read.json("/data/bike.json").na.drop()
    df.registerTempTable("bikeDataTbl")
   val df1= Sparksession.sql("select " +
      " cast(instant as Double)," +
      " dteday, " +
      "cast (season as Double) season, " +
      "cast(yr as Double) yr, " +
      "cast(mnth as Double) mnth, " +
      "cast (hr as Double) hr, " +
      "cast (holiday as Double) holiday, " +
      "cast (weekday as int) weekday, " +
      "cast (workingday as Double) workingday,    " +
      "cast (weathersit as Double) weathersit, " +
      "cast (temp as Double) temp ,cast (atemp as Double) atemp, " +
      "cast (hum as Double) hum, cast (windspeed as Double) windspeed,        " +
      "cast (casual as Double) casual, cast (registered as Double) registered, " +
      "cast (cnt as Double) label from bikeDataTbl")
    df1
  }

  def loadTestgData(Sparksession:SparkSession):DataFrame = {
    val df = Sparksession.read.json("/data/bikeTest.json").na.drop()
    df.registerTempTable("bikeDataTbl")
    Sparksession.sql("select instant,dteday, " +
      "cast (season as Double) season, cast(yr as Double) yr, " +
      "cast(mnth as Double) mnth, cast (hr as Double) hr, cast (holiday as Double) holiday, " +
      "cast (weekday as Double ) weekday, cast (workingday as Double) workingday,    " +
      "cast (weathersit as Double) weathersit, cast (temp as Double) temp ," +
      "cast (atemp as Double) atemp, cast (hum as Double) hum, " +
      "cast (windspeed as Double) windspeed,        " +
      "cast (casual as Double) casual, cast (registered as Double) registered, " +
      "cast (cnt as Double)  label from bikeDataTbl")
  }

}