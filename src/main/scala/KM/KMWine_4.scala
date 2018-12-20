package KM

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{SparkSession, _}

object KMWine_4 {
  
def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
       val sparkSession = SparkSession.builder
          .master("local")
          .appName("Wine Kmean-app")
         .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
         .getOrCreate()
                         
  //====read wine data from json
  val df2=loadTrainingData(sparkSession)
  //df2.printSchema()

  //======build the model
  val kMeansPredictionModel = getKMModel(df2)
  
  //===using this Model show cluster of each data
  val predictionResult = kMeansPredictionModel.transform(df2)
  predictionResult.show()
        
  //====predict cluster which it blong to
 val predictNewData = TestModel(sparkSession,kMeansPredictionModel)
   predictNewData.show()
}
  //read data and return dataframe
  def loadTrainingData(spark:SparkSession):DataFrame = {
    spark.read.json("/data/winedata.json").na.drop()
  }
 //Build PiplLine and return model
  def getKMModel(df:DataFrame) = {
        //Combining all (required) numerical  features into a single feature vector
         val assembler = new VectorAssembler().setInputCols(Array("Type","Alcohol","Malic","Ash","Alcalinity","Magnesium","Phenols","Flavanoids","Nonflavanoids","Proanthocyanins","Color","Hue","Dilution","Proline")).setOutputCol("features")
        //Fitting a K-Means model on the extracted features
        val kmeans = new KMeans().setK(4).setFeaturesCol("features").setPredictionCol("prediction")
        val pipeline = new Pipeline().setStages(Array(assembler, kmeans))
        val kMeansPredictionModel = pipeline.fit(df)
        kMeansPredictionModel
 }
  //return model resuls from input data
  def TestModel(spark:SparkSession,model:PipelineModel ):DataFrame = {
     val df = spark.read.json("/data/winedataTest.json").na.drop()
     model.transform(df)
  }

}