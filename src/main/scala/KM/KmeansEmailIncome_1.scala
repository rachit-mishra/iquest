package KM

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession


object KmeansEmailIncome_1 {
  def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
  
     val sparkSession = SparkSession.builder
           .master("local")
           .appName("KmeansEmail")
           .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
           .getOrCreate()
      val sqlContext = new org.apache.spark.sql.SQLContext(sparkSession.sparkContext)
      val input = sqlContext.createDataFrame(Seq(
         ("a@email.com", 12000,"M"),
         ("b@email.com", 43000,"M"),
         ("c@email.com", 5000,"F"),
         ("5c@email.com", 50000,"F"),
         ("99c@email.com", 588000,"F"),
         ("d@email.com", 60000,"M"),
         ("d1@email.com", 60000,"U")
        )).toDF("email", "income","gender")
        
        input.show()
        
        //convert M and F to 0.0 and 1.0 
        //Converting categorical attribute labels into label indexes
        val genindexer = new StringIndexer().setInputCol("gender").setOutputCol("genderIndex")
        //OneHotEncoder transforms a column with a label index into a column of vectored features
        //Converting categorical label indexes into numerical vectors
        val genencoder = new OneHotEncoder().setInputCol("genderIndex").setOutputCol("genderVec")

       // val zoneindexer1 = new StringIndexer().setInputCol("zone").setOutputCol("zoneIndex")
       // val zoneencoder1 = new OneHotEncoder().setInputCol("zoneIndex").setOutputCol("zoneVec")

        //Combining all numerical features into a single feature vector
        val assembler = new VectorAssembler()
          .setInputCols(Array("income","genderVec"))
          .setOutputCol("features")
        
        //Fitting a K-Means model on the extracted features
        val kmeans = new KMeans().setK(3)
          .setFeaturesCol("features")
          .setPredictionCol("clusterNumber")

        val pipeline = new Pipeline()
         .setStages(Array(genindexer, genencoder,assembler, kmeans))
        //The model
        //Predicting using K-Means model to get clusters for each data row
        val kMeansPredictionModel = pipeline.fit(input)

    //centeroids
     println("==========Centroids Start=========")
     kMeansPredictionModel.stages(3)  ///postion of kmeans is 3 in Pipeline Array stages
       .asInstanceOf[KMeansModel]
       .clusterCenters
       .foreach { println }

    println("==========Centroids End=========")

    //show cluster number belong to all data
    val predictionResult = kMeansPredictionModel.transform(input)
    predictionResult.show()

    //=========New data. Show Cluster
    val input2 = sqlContext.createDataFrame(Seq(
                ("a@email.com", 20000000,"F")
               )).toDF("email", "income","gender")
    val predictionResult2 = kMeansPredictionModel.transform(input2)
    predictionResult2.show()
   }

}