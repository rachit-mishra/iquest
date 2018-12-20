package classification

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.types.{StructType, StructField, LongType}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}
import org.apache.spark.sql.types.{StructType, StructField, DoubleType}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler


/**
  * Created by Ravee on 8/17/2016.
  */
object RandomForest {

  val logger = Logger.getLogger(getClass.getName)
  def main(args:Array[String])    {

    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()

    val sc = spark.sparkContext
    val df1 = spark.read.json("cancer.json")

    //df1.printSchema()
    val df2 = df1.filter("bareNuclei is not null")

    val assembler = new VectorAssembler().setInputCols(Array("bareNuclei",
      "blandChromatin", "clumpThickness", "marginalAdhesion", "mitoses",
      "normalNucleoli", "singleEpithelialCellSize", "uniformityOfCellShape",
      "uniformityOfCellSize")).setOutputCol("features")
    val df3 = assembler.transform(df2)

    val labelIndexer = new StringIndexer().setInputCol("class").setOutputCol("label")
    val df4 = labelIndexer.fit(df3).transform(df3)
   // df4.show

    val splitSeed = 5043
    val Array(trainingData, testData) = df4.randomSplit(Array(0.7, 0.3), splitSeed)

    val classifier = new RandomForestClassifier()
      .setImpurity("gini")
      .setMaxDepth(3)
      .setNumTrees(20)
      .setFeatureSubsetStrategy("auto")
      .setSeed(5043)
    val model = classifier.fit(trainingData)

    val predictions = model.transform(testData)
    predictions.select("sampleCodeNumber", "label", "prediction").show(5)


    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)

   print("accuracy===========================>>"+accuracy)

  }
}
