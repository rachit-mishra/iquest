package linear

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

object LinearRegressionPipeline {
  def main(args: Array[String]) {

    // STEP 1:
    // Load the Spark context and create the SQL Context
    val conf = new SparkConf().setAppName("Regression")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // STEP 2:
    // Load the Raw training data. The spark_csv package has
    // a utility method to create a DataFrame from
    // the csv. The file is assumed to have a header row.
    // We would like to try and detect / infer the schema
    val trainingDataRaw = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("auto-mpg.csv")

    // STEP 3:
    // Create an array of strings with the features columns. Needed to create feature vector
    // The first column (mpg) is the label, ignore it for selecting features
    val featuresArr = trainingDataRaw.columns.drop(1)

    // Step 4:
    // Use a vector assembler to create a features vector.
    // The training data (DataFrame) needs a “features” vector for running
    // any machine learning algorithm. The name of the new output column
    // is “features”. The machine learning library needs individual feature
    // columns to be consolidated into one “Vector” column. This step is a
    // preparation for the conversion
    val featuresAssembler = new VectorAssembler()
      .setInputCols(featuresArr)
      .setOutputCol("features")

    // STEP 5:
    // This will create one column called “features” which is a vector column
    // and add it into the trainingData DataFrame. This column will be used
    // for the analysis.
    val trainingData = featuresAssembler.transform(trainingDataRaw)

    // STEP 6:
    // Create a linear regression object. Set maximum iterations to 1000 for convergence
    // Set the values for regularisation parameters and elastic net parameters
    // Let the model know that the training data frame has a column called “features&amp;amp;quot;
    // We want to use “mpg” as the label column and output the prediction results later
    // into a column called “predicted_mpg”. This only happens later, when we predict
    // Not necessary while training the model.
    val lr = new LinearRegression().setMaxIter(1000)
      .setRegParam(0.3).setElasticNetParam(0.8)
      .setFeaturesCol("features").setLabelCol("mpg")
      .setPredictionCol("predicted_mpg")

    // STEP 7:
    // Train the “Linear Regression” model with the prepared training data.
    // The “fit” method trains the model. This is done, we can now use the
    // lrModel to predict using the “transform” method on any new data
    val lrModel = lr.fit(trainingData)

    // STEP 8:
    // Print out some relevant details for the created model

    // The “intercept” corresponds to 𝛉0 and
    // the coefficients correspond to 𝛉1, 𝛉2, 𝛉3, …, 𝛉d

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")

    // Values of J(theta) over each iteration.
    println("objectiveHistory: ${trainingSummary.objectiveHistory.toList}")

    // Values of residuals. Important while analysing “correctness" of model
    trainingSummary.residuals.show()

    // More statistics. To be discussed in a separate blog post
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    // STEP 9:
    // Now use the model to predict. Ideally this SHOULD NOT be part of the training data.
    // We use one row from the same training dataset just for convenience
    // Pick the first row in the set
    val testFrame = trainingData.limit(1)

    // Predict and show the result
    val predictedFrame = lrModel.transform(testFrame)
    predictedFrame.select("predicted_mpg").show()

  }
}