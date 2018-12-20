package linear

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

/**
  * Created by Ravee on 2/20/2017.
  */
object LinerTeach {


  val stateHolidayIndexer = new StringIndexer()
    .setInputCol("StateHoliday")
    .setOutputCol("StateHolidayIndex")
  val schoolHolidayIndexer = new StringIndexer()
    .setInputCol("SchoolHoliday")
    .setOutputCol("SchoolHolidayIndex")
  val stateHolidayEncoder = new OneHotEncoder()
    .setInputCol("StateHolidayIndex")
    .setOutputCol("StateHolidayVec")
  val schoolHolidayEncoder = new OneHotEncoder()
    .setInputCol("SchoolHolidayIndex")
    .setOutputCol("SchoolHolidayVec")
  val dayOfMonthEncoder = new OneHotEncoder()
    .setInputCol("DayOfMonth")
    .setOutputCol("DayOfMonthVec")
  val dayOfWeekEncoder = new OneHotEncoder()
    .setInputCol("DayOfWeek")
    .setOutputCol("DayOfWeekVec")
  val storeEncoder = new OneHotEncoder()
    .setInputCol("Store")
    .setOutputCol("StoreVec")

  val assembler = new VectorAssembler()
    .setInputCols(Array("StoreVec", "DayOfWeekVec", "Open",
      "DayOfMonthVec", "StateHolidayVec", "SchoolHolidayVec"))
    .setOutputCol("features")
  val logger = Logger.getLogger(getClass.getName)

  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

  def main(args: Array[String]) ={
    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
      .getOrCreate()

  import spark.implicits._
    val data = loadTrainingData(spark)
    val Array(testRaw, testData) = loadKaggleTestData(spark)

    // The linear Regression Pipeline
    val linearTvs = preppedLRPipeline()
    logger.info("evaluating linear regression")

    val lrModel = fitModel(linearTvs, data)
    logger.info("Generating kaggle predictions")

    val lrOut = lrModel.transform(testData)
      .withColumnRenamed("prediction","Sales")
      .withColumnRenamed("Store","PredId")
      .select("PredId", "Sales")

    savePredictions(lrOut, testRaw)

}

  def preppedLRPipeline():TrainValidationSplit = {
    val lr = new LinearRegression()

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.25, 0.5, 0.75, 1.0))
      .build()

    val pipeline = new Pipeline()
      .setStages(Array(stateHolidayIndexer, schoolHolidayIndexer,
        stateHolidayEncoder, schoolHolidayEncoder, storeEncoder,
        dayOfWeekEncoder, dayOfMonthEncoder,
        assembler, lr))

    val tvs = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.75)
    tvs
  }

  def loadTrainingData(spark:SparkSession):DataFrame = {
    val trainRaw = spark
      .read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load("data/train.csv")
      .repartition(6)
    trainRaw.registerTempTable("raw_training_data")

    spark.sql("""SELECT
        CAST (Sales as DOUBLE) label,  CAST (Store as DOUBLE) Store,  CAST (Open as DOUBLE),
        CAST (DayOfWeek as DOUBLE),
        StateHoliday, SchoolHoliday,  CAST (regexp_extract(Date, '\\d+-\\d+-(\\d+)', 1) as DOUBLE) DayOfMonth
        FROM raw_training_data""").na.drop()

  }

  def loadKaggleTestData(spark:SparkSession) = {
    val testRaw = spark
      .read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load("data/trainsample.csv")
      .repartition(6)
    testRaw.registerTempTable("raw_test_data")

    val testData = spark.sql("""SELECT
    CAST (Sales as DOUBLE) label,  CAST (Store as DOUBLE) Store,  CAST (Open as DOUBLE),
    CAST (DayOfWeek as DOUBLE), StateHoliday, SchoolHoliday,  CAST (regexp_extract(Date, '\\d+-\\d+-(\\d+)', 1) as DOUBLE) DayOfMonth
    FROM raw_test_data
    WHERE !(ISNULL(Store) OR ISNULL(Open) OR ISNULL(DayOfWeek)
      OR ISNULL(StateHoliday) OR ISNULL(SchoolHoliday))
                                  """).na.drop()
    // weird things happen if you don't filter out the null values manually

    Array(testRaw, testData) // got to hold onto testRaw so we can make sure
    // to have all the prediction IDs to submit to kaggle
  }

  def savePredictions(predictions:DataFrame, testRaw:DataFrame) = {
    val tdOut = testRaw
      .select("Store")
      .distinct()
      .join(predictions, testRaw("Store") === predictions("PredId"), "outer")
      .select("Store", "Sales")
      .na.fill(0:Double) // some of our inputs were null so we have to
    // fill these with something
    tdOut
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("linear_regression_predictions.csv")
  }

  def fitModel(tvs:TrainValidationSplit, data:DataFrame) = {
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
    logger.info("Test MSE:")
    logger.info(rm.meanSquaredError)
    logger.info("Test RMSE:")
    logger.info(rm.rootMeanSquaredError)

    model
  }


}