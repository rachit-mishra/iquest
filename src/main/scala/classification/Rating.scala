package classification

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
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.mllib.classification.NaiveBayes  
import org.apache.spark.mllib.regression.LabeledPoint  
import org.apache.spark.{SparkContext, SparkConf}  
import org.apache.spark.mllib.feature.{IDF, HashingTF}

object Rating {  
  def main(args:Array[String])    {

        System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
        
          val sparkSession = SparkSession.builder
            .master("local")
            .appName("my-spark-app")
            .config("spark.some.config.option", "config-value")
            .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
            .getOrCreate()
                          
      val sc = sparkSession.sparkContext

      /*
        This loads the data from HDFS.
        HDFS is a distributed file storage system so this technically 
        could be a very large multi terabyte file
      */      
      val dataFile = sc.textFile("movieRating.txt")

      /*
        HashingTF and IDF are helpers in MLlib that helps us vectorize our
        synopsis instead of doing it manually
      */       
      val hashingTF = new HashingTF()

      /*
        Our ultimate goal is to get our data into a collection of type LabeledPoint.
        The MLlib implementation uses LabeledPoints to train the Naive Bayes model.
        First we parse the file for ratings and vectorize the synopses
       */

      val ratings=dataFile.map{ x=>  x.split(";")
        match {
          case Array(rating,synopsis) =>  rating.toDouble
        }
      }

      val synopsis_frequency_vector=dataFile.map{x=>
        x.split(";")
        match {
          case Array(rating,synopsis) =>
            val stemmed=Stemmer.tokenize(synopsis)
            hashingTF.transform(stemmed)
        }
      }

      synopsis_frequency_vector.cache()

      /*
       http://en.wikipedia.org/wiki/Tf%E2%80%93idf
       https://spark.apache.org/docs/1.3.0/mllib-feature-extraction.html
      */
      val idf = new IDF().fit(synopsis_frequency_vector)
      val tfidf=idf.transform(synopsis_frequency_vector)

      /*produces (rating,vector) tuples*/
      val zipped=ratings.zip(tfidf)

      /*Now we transform them into LabeledPoints*/
      val labeledPoints = zipped.map{case (label,vector)=> LabeledPoint(label,vector)}

      val model = NaiveBayes.train(labeledPoints)

      /*--- Model is trained now we get it to classify our test file with only synopsis ---*/
      val testDataFile = sc.textFile("movieratingTest.txt")

      /*We only have synopsis now. The rating is what we want to achieve.*/
      val testVectors=testDataFile.map{x=>
        val stemmed=Stemmer.tokenize(x)
        hashingTF.transform(stemmed)
      }
      testVectors.cache()

      val tfidf_test = idf.transform(testVectors)

      val result = model.predict(tfidf_test)

      result.collect.foreach(x=>println("Predicted rating for the movie is: "+x))

    }
}