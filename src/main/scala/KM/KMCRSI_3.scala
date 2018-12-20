package KM

import org.apache.log4j.Logger
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{SparkSession, _}

object KMCRSI_3 {
  case class CC1(ID: String, LABEL: String, RTN5:java.lang.Double=0.0,
                  FIVE_DAY_GL:java.lang.Double=0.0,
                  CLOSE:java.lang.Double=0.0, RSI2:java.lang.Double=0.0,
                  RSI_CLOSE_3:java.lang.Double=0.0, PERCENT_RANK_100:java.lang.Double=0.0,
                  RSI_STREAK_2:java.lang.Double=0.0, CRSI:java.lang.Double=0.0)

  val logger = Logger.getLogger(getClass.getName)
  
def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
     //session
     val spark = SparkSession.builder
       .master("local")
       .appName("KMCRSI3")
       .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
       .getOrCreate()

  //-----------read data from json
  val df2=loadTrainingData(spark)

  //-----------Build model
  val kMeansPredictionModel = getKMModel(df2)

  //-----------Predict from Model
  val predictionResult = kMeansPredictionModel.transform(df2)
  predictionResult.show()
  //
  predictionResult.createOrReplaceTempView("PridictionTable")
  val clusterCount1= spark.sql("select clsuterNumbers, count(features)" +
                             "from PridictionTable group by clsuterNumbers")

  // val clusterCount = predictionResult.rdd.countByValue()
  println("=================Print Data Points in each Cluster====>"+clusterCount1.rdd.foreach(println))

  val WSSSE = kMeansPredictionModel.stages(3).asInstanceOf[KMeansModel].computeCost(predictionResult);
  System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

  //-----------Test the model
  val predictNewData = TestModel(spark,kMeansPredictionModel)
   predictNewData.show()
}



  //read data and return dataframe
  def loadTrainingData(spark:SparkSession):DataFrame = {
    spark.read.json("/data/CRSI2.json").na.drop()
  }

  //Build PiplLine and return model
  def getKMModel(df:DataFrame): PipelineModel= {
        val indexer = new StringIndexer().setInputCol("LABEL").setOutputCol("LABELIndex")
        val encoder = new OneHotEncoder().setInputCol("LABELIndex").setOutputCol("LABELVec")

        //Combining all (required) numerical  features into a single feature vector
         val assembler = new VectorAssembler()
                .setInputCols(Array( "RSI2", "RSI_CLOSE_3", "PERCENT_RANK_100", "RSI_STREAK_2", "CRSI"))
                .setOutputCol("features")

        //Fitting a K-Means model on the extracted features
        val kmeans = new KMeans().setK(3).setFeaturesCol("features").setPredictionCol("clsuterNumbers")
        val pipeline = new Pipeline().setStages(Array(indexer,encoder,assembler, kmeans))
        val kMeansPredictionModel = pipeline.fit(df)
        kMeansPredictionModel
  }
   
  //return model resuls from input data  
  def TestModel(spark:SparkSession,model:PipelineModel ):DataFrame = {
     val df = spark.read.json("/data/CRSI2.json").na.drop()
     model.transform(df)
  }


}