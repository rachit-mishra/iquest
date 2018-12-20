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


object csvtest {
  
 
def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
  
     val sparkSession = SparkSession.builder
                          .master("local")
                          .appName("my-spark-app")
                          .config("spark.some.config.option", "config-value")
                          .getOrCreate()
                         
  //sql context                        
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkSession.sparkContext)
  import sqlContext.implicits._
  
    val data = loadTrainingData(sqlContext)
 data.printSchema()
}

def loadTrainingData(sqlContext:SQLContext):DataFrame = {
    val schema = StructType(Array(
                      StructField("Store",DoubleType,true),
                      StructField("DayOfWeek",DoubleType,true),
                      StructField("Date",DoubleType,true),
                      StructField("Sales",DoubleType,true),
                      StructField("Customers",IntegerType,true),
                      StructField("Open",IntegerType,true),
                      StructField("Promo",IntegerType,true),
                      StructField("StateHoliday",StringType,true),
                      StructField("SchoolHoliday",StringType,true)
  ))
  
 // "Store","DayOfWeek","Date"      ,"Sales",  "Customers","Open","Promo","StateHoliday","SchoolHoliday"
 //  1,     5,           2015-07-31,  5263,    555,          1,    1,      "0",             "1"

  val trainRaw = sqlContext
    .read.format("com.databricks.spark.csv")
    .option("header", "true")
    //.option("inferSchema", "true")
    .load("trainsample.csv")
    .repartition(6)
    
  val schemaString=  trainRaw.head()
  trainRaw.registerTempTable("raw_training_data")

//  sqlContext.sql("""SELECT
//    double(Sales) label, double(Store) Store, int(Open) Open, double(DayOfWeek) DayOfWeek,
//    StateHoliday, SchoolHoliday, (double(regexp_extract(Date, '\\d+-\\d+-(\\d+)', 1))) DayOfMonth
//    FROM raw_training_data
//  """).na.drop()
  
    sqlContext.sql("""SELECT
      CAST (Sales as DOUBLE) label,  CAST (Store as DOUBLE) Store,  CAST (Open as DOUBLE),   CAST (DayOfWeek as DOUBLE),
    StateHoliday, SchoolHoliday,  CAST (regexp_extract(Date, '\\d+-\\d+-(\\d+)', 1) as DOUBLE) DayOfMonth
    FROM raw_training_data
  """).na.drop()
}

  

}