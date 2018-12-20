package logistic

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class Obs1(id:Integer,thickness: Double, size: Double, shape: Double, madh: Double,
               epsize: Double, bnuc: Double, bchrom: Double, nNuc: Double, mit: Double,clas: Double)
/**
  * 1. Sample code number: id number
  * 2. Clump Thickness: 1 - 10
  * 3. Uniformity of Cell Size: 1 - 10
  * 4. Uniformity of Cell Shape: 1 - 10
  * 5. Marginal Adhesion: 1 - 10
  * 6. Single Epithelial Cell Size: 1 - 10
  * 7. Bare Nuclei: 1 - 10
  * 8. Bland Chromatin: 1 - 10
  * 9. Normal Nucleoli: 1 - 10
  * 10. Mitoses: 1 - 10
  * 11. Class: (2 for benign, 4 for malignant)
**/

/**
  * Created by Ravee on 2/13/2017.
  */
object CancerTestDataSet {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._

    val rdd = spark
      .read.format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .load("data/breastCancer.txt").toDF("id","thickness", "size", "shape","madh","epsize","bnuc",
      "bchrom","nNuc","mit","clas")
    rdd.printSchema()
    rdd.registerTempTable("df")
   val rdd1= spark.sql("select id,thickness, size, shape,madh,epsize,cast(bnuc as double),bchrom,nNuc,mit," +
     "clas from df where bnuc !='?' ").as[Obs1]
   // val obsRDD = parseRDD(rdd).map(parseObs)
    val obsDF = rdd1.toDF().cache()

    obsDF.registerTempTable("obs")
    obsDF.printSchema
    obsDF.show
    //  describe computes statistics for thickness column, including count, mean, stddev, min, and max
    obsDF.describe("thickness").show
    // compute the avg thickness, size, shape grouped by clas (malignant or not)
    spark.sql("SELECT clas, avg(thickness) as avgthickness, avg(size) as avgsize, avg(shape) as avgshape " +
      "FROM obs GROUP BY clas ").show

    // compute avg thickness grouped by clas (malignant or not)
    obsDF.groupBy("clas").avg("thickness").show

    //define the feature columns to put in the feature vector
    val featureCols = Array("thickness", "size", "shape", "madh", "epsize", "bnuc", "bchrom", "nNuc", "mit")

    //set the input and output column names
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    //return a dataframe with all of the  feature columns in  a vector column
    val df2 = assembler.transform(obsDF)
    // the transform method produced a new column: features.
    df2.show

    //  Create a label column with the StringIndexer
    val labelIndexer = new StringIndexer().setInputCol("clas").setOutputCol("label")
    val df3 = labelIndexer.fit(df2).transform(df2)
    // the  transform method produced a new column: label.
    df3.show

    //  split the dataframe into training and test data
    val splitSeed = 5043
    val Array(trainingData, testData) = df3.randomSplit(Array(0.7, 0.3), splitSeed)

    // create the classifier,  set parameters for training
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    //  use logistic regression to train (fit) the model with the training data
    val model = lr.fit(trainingData)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    // run the  model on test features to get predictions
    val predictions = model.transform(testData)
    //As you can see, the previous model transform produced a new columns: rawPrediction, probablity and prediction.
    predictions.show

    //A common metric used for logistic regression is area under the ROC curve (AUC).
    // We can use the BinaryClasssificationEvaluator to obtain the AUC
    // create an Evaluator for binary classification, which expects two input columns: rawPrediction and label.
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
      .setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")

    // Evaluates predictions and returns a scalar metric areaUnderROC(larger is better).
    val accuracy = evaluator.evaluate(predictions)
    print("accuracy===="+accuracy+"\n")

    // ====================================Calculate Metrics
    val lp = predictions.select( "label", "prediction")
    print("lp===="+lp+"\n")

    val counttotal = predictions.count()
    print("counttotal===="+counttotal+"\n")

    val correct = lp.filter($"label" === $"prediction"+"\n").count()
    print("correct===="+correct+"\n")

    val wrong = lp.filter(not($"label" === $"prediction")).count()
    print("wrong===="+wrong+"\n")

    val truep = lp.filter($"prediction" === 0.0).filter($"label" === $"prediction").count()
    print("truep===="+truep+"\n")

    val falseN = lp.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count()
    print("falseN===="+falseN+"\n")

    val falseP = lp.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count()
    print("falseP===="+falseP+"\n")

    val ratioWrong=wrong.toDouble/counttotal.toDouble
    print("ratioWrong===="+ratioWrong+"\n")

    val ratioCorrect=correct.toDouble/counttotal.toDouble
    print("ratioCorrect===="+ratioCorrect+"\n")

  }


  def parseObs(line: Array[Double]): Obs = {
    Obs(
      if (line(9) == 4.0) 1 else 0, line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7),
      line(8)
    )
  }
  // function to transform an RDD of Strings into an RDD of Double, filter lines with ?, remove first column
  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(",")).filter(_ (6) != "?").map(_.drop(1)).map(_.map(_.toDouble))
  }


}
