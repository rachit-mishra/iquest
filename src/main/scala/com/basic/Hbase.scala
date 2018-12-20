package com.basic

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
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

object Hbase {

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

    //Hbase configuration
    System.setProperty("user.name", "hdfs")
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.master", "localhost:60000")
    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("zookeeper.znode.parent", "/hbase")

    //
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
    val ErrorROWDS = ErrorROWs.toDS()

    //
    val tripDataTbl = "tripdata"
    conf.set(TableInputFormat.INPUT_TABLE, tripDataTbl)
    val tripRDD = sparkSession.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val tripResult = tripRDD.map(tuple => tuple._2)
    val tripRows = tripResult.map(row =>
      {
        val p0 = row.getRow.map(_.toChar).mkString.toString()
        val p1 = Bytes.toString(row.getValue(Bytes.toBytes("m"), Bytes.toBytes("startdt")))
        TridataRow(p0, p1)
      })
    //convet RDD to DS
    val TripDS = tripRows.toDS()
    val TripDF = tripRows.toDF()
    TripDF.show

    //
    val deviceInfoTbl = "device"
    conf.set(TableInputFormat.INPUT_TABLE, deviceInfoTbl)
    val deviceRDD = sparkSession.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val deviceResult = deviceRDD.map(tuple => tuple._2)
      .map { row =>
        {
          val p0 = row.getRow.map(_.toChar).mkString.toString()
          val p1 = Bytes.toString(row.getValue(Bytes.toBytes("m"), Bytes.toBytes("prct")))
          DeviceRow(p0, p1)
        }
      }
    //convet RDD to DS
    val deviceDS = deviceResult.toDS()
    val deviceDF = deviceResult.toDF()
    deviceDS.show

    //using method
    val deviceResultNew = deviceRDD.map(tuple => tuple._2).map(row => device_Result(row)).toDF()
    deviceResultNew.show()

    //Join Three Hbase tables. Compare same with Hive tables on top Hbase tables.
    val TripErrordf = ErrorROWDF.join(TripDF, "rowkey")
    val errorTripdevice = TripErrordf.join(deviceDF, "rowkey")
    errorTripdevice.show

    //store data in HDFS
    errorTripdevice.write.parquet("hdfs:master:8020/tmp/error.parquet")
  }

  def device_Result(row: Result): Hbase.DeviceRow = {
    val p0 = row.getRow.map(_.toChar).mkString.toString()
    val p1 = Bytes.toString(row.getValue(Bytes.toBytes("m"), Bytes.toBytes("prct")))
    DeviceRow(p0, p1)
  }

}