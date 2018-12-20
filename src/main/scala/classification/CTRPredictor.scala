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
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.types.{StructType, StructField, LongType}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}
import org.apache.spark.sql.types.{StructType, StructField, DoubleType}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}

object CTRPredictor {
   
val logger = Logger.getLogger(getClass.getName)

def main(args:Array[String])    {

        System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
     val spark = SparkSession.builder
                          .master("local")
                          .appName("CTRPredictor")
                          .config("spark.some.config.option", "config-value")
                          .getOrCreate()
      val sc = spark.sparkContext
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import spark.implicits._

  val data = sqlContext
    .read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("ctrPredictor.txt")
    .repartition(6)
  // data.printSchema() 
  
   val dd2 =data.selectExpr("cast(Label as double) label", 
                           "cast(IntFeature1 as double) IntFeature1",
                           "cast(IntFeature2 as double) IntFeature2",
                           "cast(IntFeature3 as double) IntFeature3",
                           "cast(IntFeature4 as double) IntFeature4",
                           "cast(IntFeature5 as double) IntFeature5",
                           "cast(IntFeature6 as double) IntFeature6",
                           "cast(IntFeature7 as double) IntFeature7",
                           "cast(IntFeature8 as double) IntFeature8",
                           "cast(IntFeature9 as double) IntFeature9",
                           "cast(IntFeature10 as double) IntFeature10",
                           "cast(IntFeature11 as double) IntFeature11",
                           "cast(IntFeature12 as double) IntFeature12",
                           "cast(IntFeature13 as double) IntFeature13",
                           "CatFeature1","CatFeature2", "CatFeature3",
                           "CatFeature4",	"CatFeature5","CatFeature6","CatFeature7", "CatFeature8","CatFeature9",
                           "CatFeature10","CatFeature11","CatFeature12","CatFeature13", "CatFeature14","CatFeature15",
                           "CatFeature16","CatFeature17", "CatFeature18",
                           "CatFeature19","CatFeature20", "CatFeature21",
                           "CatFeature22","CatFeature23", "CatFeature24", "CatFeature25","CatFeature26")
                           .na.replace("CatFeature1", Map( "" -> "NA"))
                           .na.replace("CatFeature2", Map( "" -> "NA"))
                           .na.replace("CatFeature3", Map( "" -> "NA"))
                           .na.replace("CatFeature4", Map( "" -> "NA"))
                           .na.replace("CatFeature5", Map( "" -> "NA"))
                           .na.replace("CatFeature6", Map( "" -> "NA"))
                           .na.replace("CatFeature7", Map( "" -> "NA"))
                           .na.replace("CatFeature8", Map( "" -> "NA"))
                           .na.replace("CatFeature9", Map( "" -> "NA"))
                           .na.replace("CatFeature10", Map( "" -> "NA"))
                           .na.replace("CatFeature11", Map( "" -> "NA"))
                           .na.replace("CatFeature12", Map( "" -> "NA"))
                           .na.replace("CatFeature13", Map( "" -> "NA"))
                           .na.replace("CatFeature14", Map( "" -> "NA"))
                           .na.replace("CatFeature15", Map( "" -> "NA"))
                           .na.replace("CatFeature16", Map( "" -> "NA"))
                           .na.replace("CatFeature17", Map( "" -> "NA"))
                           .na.replace("CatFeature18", Map( "" -> "NA"))
                           .na.replace("CatFeature19", Map( "" -> "NA"))
                           .na.replace("CatFeature20", Map( "" -> "NA"))
                           .na.replace("CatFeature21", Map( "" -> "NA"))
                           .na.replace("CatFeature22", Map( "" -> "NA"))
                           .na.replace("CatFeature23", Map( "" -> "NA"))
                           .na.replace("CatFeature24", Map( "" -> "NA"))
                           .na.replace("CatFeature25", Map( "" -> "NA"))
                           .na.replace("CatFeature26", Map( "" -> "NA"))                           
                           .na.drop()
                     
      
  dd2.take(400)
   
  val cat1Indexer = new StringIndexer().setInputCol("CatFeature1").setOutputCol("indexedCat1").setHandleInvalid("skip")
  val cat1Encoder = new OneHotEncoder().setInputCol("indexedCat1") .setOutputCol("CatVector1")

  val cat2Indexer = new StringIndexer().setInputCol("CatFeature2").setOutputCol("indexedCat2").setHandleInvalid("skip")
  val cat2Encoder = new OneHotEncoder().setInputCol("indexedCat2") .setOutputCol("CatVector2")
  
  val cat3Indexer = new StringIndexer().setInputCol("CatFeature3").setOutputCol("indexedCat3").setHandleInvalid("skip")
  val cat3Encoder = new OneHotEncoder().setInputCol("indexedCat3") .setOutputCol("CatVector3")
  
  val cat4Indexer = new StringIndexer().setInputCol("CatFeature4").setOutputCol("indexedCat4").setHandleInvalid("skip")
  val cat4Encoder = new OneHotEncoder().setInputCol("indexedCat4") .setOutputCol("CatVector4")
  
  val cat5Indexer = new StringIndexer().setInputCol("CatFeature5").setOutputCol("indexedCat5").setHandleInvalid("skip")
  val cat5Encoder = new OneHotEncoder().setInputCol("indexedCat5") .setOutputCol("CatVector5")
  
  val cat6Indexer = new StringIndexer().setInputCol("CatFeature6").setOutputCol("indexedCat6").setHandleInvalid("skip")
  val cat6Encoder = new OneHotEncoder().setInputCol("indexedCat6") .setOutputCol("CatVector6")
  
  val cat7Indexer = new StringIndexer().setInputCol("CatFeature7").setOutputCol("indexedCat7").setHandleInvalid("skip")
  val cat7Encoder = new OneHotEncoder().setInputCol("indexedCat7") .setOutputCol("CatVector7")
  
  val cat8Indexer = new StringIndexer().setInputCol("CatFeature8").setOutputCol("indexedCat8").setHandleInvalid("skip")
  val cat8Encoder = new OneHotEncoder().setInputCol("indexedCat8") .setOutputCol("CatVector8")
  
  val cat9Indexer = new StringIndexer().setInputCol("CatFeature9").setOutputCol("indexedCat9").setHandleInvalid("skip")
  val cat9Encoder = new OneHotEncoder().setInputCol("indexedCat9") .setOutputCol("CatVector9")
  
  val cat10Indexer = new StringIndexer().setInputCol("CatFeature10").setOutputCol("indexedCat10").setHandleInvalid("skip")
  val cat10Encoder = new OneHotEncoder().setInputCol("indexedCat10") .setOutputCol("CatVector10")
  
  val cat11Indexer = new StringIndexer().setInputCol("CatFeature11").setOutputCol("indexedCat11").setHandleInvalid("skip")
  val cat11Encoder = new OneHotEncoder().setInputCol("indexedCat11") .setOutputCol("CatVector11")
  
  val cat12Indexer = new StringIndexer().setInputCol("CatFeature12").setOutputCol("indexedCat12").setHandleInvalid("skip")
  val cat12Encoder = new OneHotEncoder().setInputCol("indexedCat12") .setOutputCol("CatVector12")
  
  val cat13Indexer = new StringIndexer().setInputCol("CatFeature13").setOutputCol("indexedCat13").setHandleInvalid("skip")
  val cat13Encoder = new OneHotEncoder().setInputCol("indexedCat13") .setOutputCol("CatVector13")
  
  
  val assembler = new VectorAssembler().setInputCols(Array("CatVector1","CatVector2","CatVector3","CatVector4","CatVector5","CatVector6","CatVector7","CatVector8","CatVector9","CatVector10","CatVector11","CatVector12","CatVector13"))  .setOutputCol("features")
  
 val lr = new LogisticRegression()
 val pipeline = new Pipeline().setStages(Array(cat1Indexer, cat2Indexer, cat3Indexer, cat4Indexer, cat5Indexer,cat6Indexer, cat7Indexer,cat8Indexer, cat9Indexer, cat10Indexer, cat11Indexer, cat12Indexer, cat13Indexer,cat1Encoder, cat2Encoder, cat3Encoder, cat4Encoder, cat5Encoder, cat6Encoder, cat7Encoder,cat8Encoder, cat9Encoder, cat10Encoder, cat11Encoder, cat12Encoder, cat13Encoder,assembler, lr))
                    
//val Array(dataTrain,dataTest) = dd2.randomSplit(Array(0.8,0.2),42)
//val model = pipeline.fit(dataTrain.na.drop())
//val output = model.transform(dataTest.na.drop()).select("features", "label", "prediction", "rawPrediction", "probability")
//val prediction = output.select("label", "prediction")
 
//prediction.show()
 
 }


}