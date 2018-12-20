package com.basic

import org.apache.hadoop.hbase.client.{ HBaseAdmin, Result }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellUtil
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.functions.avg
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StructType, StructField, StringType }
import org.apache.spark.sql._

object JoinMany {

  case class ErrorRow(rowkey: String, inv: String, err: String, dt: String)
  case class TridataRow(rowkey: String, startdt: String)
  case class DeviceRow(rowkey: String, prct: String)

  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()

    //--------------------------------------------------------------HBASE
    //Hbase configuration
    System.setProperty("user.name", "hdfs")
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.master", "localhost:60000")
    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("zookeeper.znode.parent", "/hbase")

    val errorTbl = "errors"
    conf.set(TableInputFormat.INPUT_TABLE, errorTbl)
    val errorsRDD = sparkSession.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val ErrorResult = errorsRDD.map(tuple => tuple._2)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sparkSession.implicits._
    val ErrorROWs = ErrorResult.map(row =>
      {
        val p0 = row.getRow.map(_.toChar).mkString.toString()
        val p1 = Bytes.toString(row.getValue(Bytes.toBytes("m"), Bytes.toBytes("err")))
        val p2 = Bytes.toString(row.getValue(Bytes.toBytes("m"), Bytes.toBytes("inv")))
        val p3 = Bytes.toString(row.getValue(Bytes.toBytes("m"), Bytes.toBytes("dt")))
        ErrorRow(p0, p1, p2, p3)
      })

    val ErrorROWDF = ErrorROWs.toDF()
    ErrorROWDF.registerTempTable("HbaseErrorROWDF")
    sparkSession.sql("select * from HbaseErrorROWDF").collect.foreach(println)

    ///------------------------------------------------------------------MYSQL
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    val MYSQL_CONNECTION_URL = "jdbc:mysql://master:3306/customer"

    val cust_reldf = sparkSession.read.format("jdbc").option("url", MYSQL_CONNECTION_URL).option("user", "root").option("password", "root").option("dbtable", "cust_rel").load()
    cust_reldf.show
    //convert DF to table
    cust_reldf.registerTempTable("MSQLcust_reltbl")
    sparkSession.sql("select * from MSQLcust_reltbl").collect.foreach(println)

    //---------------------------------------------------------------------------HIVE
    val personhive = sparkSession.sql("select * from person")
    personhive.registerTempTable("HIVEpersonTBL")
    personhive.show
    sparkSession.sql("select * from HIVEpersonTBL").collect.foreach(println)

    //-------------------------------JOIN MYSQL, HBSE,HIVE

  }

}