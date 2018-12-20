package classification

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.log4j.{Logger}

object CrossValidatorLogistLR {
   
    val logger = Logger.getLogger(getClass.getName)
def main(args:Array[String])    {

        System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
        
          val spark = SparkSession.builder
          .master("local")
          .appName("my-spark-app")
          .config("spark.some.config.option", "config-value")
          .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
          .getOrCreate()
   //data
  val trainDS = spark.createDataFrame(Seq(
   (0L, "[science] hello world", 0.0),
   (1L, "long text", 1.0),
   (2L, "[science] hello all people", 0.0),
   (3L, "[science] hello hello", 0.0)))
     .toDF("id", "text", "label")

  // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
  val tokenizer = new Tokenizer()
  .setInputCol("text")
    
  val hashingTF = new HashingTF()
  .setInputCol(tokenizer.getOutputCol)
  .setOutputCol("features")

  val lr = new LogisticRegression()
    .setMaxIter(10)

  val pipeline = new Pipeline()
    .setStages(Array(tokenizer, hashingTF, lr))
 
  val sampleModel = pipeline.fit(trainDS)
  //sampleModel.transform(trainDS).select("text", "label", "features", "prediction").show(false)
 
  val input = spark.createDataFrame(Seq(
    (4L, "Hello ScienCE"))).toDF("id", "text")

  sampleModel.transform(input).show(false)
  
  import org.apache.spark.ml.tuning.ParamGridBuilder
  val paramGrid = new ParamGridBuilder()
    .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
    .addGrid(lr.regParam, Array(0.1, 0.01))
    .build()
  
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.CrossValidator

  val cv = new CrossValidator()
    .setEstimator(pipeline) // <-- pipeline is the estimator
    .setEvaluator(new BinaryClassificationEvaluator)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(2)

  val input2 = spark.createDataFrame(Seq((4L, "Hello ambariCloud"))).toDF("id", "text")
  val cvModel = cv.fit(trainDS)

  //cvModel uses the best model found ( i.e lrModel).
  val lrModel = cvModel.bestModel
  /// below both are same.
  lrModel.transform(input2).show()
  cvModel.transform(input2).select("id", "text", "features", "prediction").show(false)
 }

}