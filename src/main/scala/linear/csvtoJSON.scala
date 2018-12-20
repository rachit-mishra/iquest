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
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.types.{StructType, StructField, LongType}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}
import org.apache.spark.sql.types.{StructType, StructField, DoubleType}

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.regression.{LinearRegression}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger}


object csvtoJSON {

def main(args: Array[String]) ={
       System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
       val spark = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .config("spark.some.config.option", "config-value")
                          .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
                          .getOrCreate()
  import spark.implicits._
  
  val data = loadTrainingData(spark)
   data.createOrReplaceTempView("dataTBl")
  spark.sql("select count(*) from table dataTBl a, dataTBl b ")
  //  data.printSchema()
    data.write.mode("overwrite").json("s3:/data/AllStateTrain.json")
  }

  def loadTrainingData(spark:SparkSession):DataFrame = {
    val df = spark
    .read.format("com.databricks.spark.csv")
    .option("header", "true")
    .load("/data/AllStateTrain.csv").na.drop()

    df
  }
}