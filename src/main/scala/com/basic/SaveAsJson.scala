package com.basic

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.mllib.feature.{StandardScalerModel, StandardScaler}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkContext, SparkConf}

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
import com.github.fommil.netlib.BLAS;

case class Home3(mlsNum: Double, city: String, sqFt: Double, bedrooms: Double, bathrooms: Double,
                    garage: Double, age: Double, acres: Double, price: Double)

object SaveAsjson extends Serializable {

  def main(args: Array[String]): Unit = {

  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
  val sparkSession = SparkSession.builder
                          .master("local")
                          .appName("SaveAsjson")
                          .getOrCreate()
  //sql context
  val sc = sparkSession.sparkContext
  val base = "homedata.txt"
  val homeData = sparkSession.read.text(base)
  import sparkSession.implicits._
  val parsed = homeData.map(line => parse(line.toString())).toDF()
    .as[Home3]
   parsed.toDF().write.format("json").save("homedata.json")
  }
  // parse home price data into case class
  def parse(line: String) = {
    val split = line.split('|')
    val mlsNum = split(0).toDouble
    val city = split(1).toString
    val sqFt = split(2).toDouble
    val bedrooms = split(3).toDouble
    val bathrooms = split(4).toDouble
    val garage = split(5).toDouble
    val age = split(6).toDouble
    val acres = split(7).toDouble
    val price = split(8).toDouble
    Home3(mlsNum, city, sqFt, bedrooms, bathrooms, garage, age, acres, price)
  }
}

  
