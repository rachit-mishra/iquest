package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.sql._


//define the schema using a case class
case class Auction(auctionid: String,
                   bid: Float, bidtime: Float, bidder: String,
                   bidderrate: Integer, openbid: Float, price: Float,
                   item: String, daystolive: Integer)

object ebay {

def main(args: Array[String]) ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
  val conf = new SparkConf().setAppName("Testing").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// Import Spark SQL data types and Row.
// load the data into a  new RDD
val ebayText = sc.textFile("data/ebay.txt")
    import sqlContext.implicits._

// create an RDD of Auction objects
val ebay = ebayText.map(line=>line.split(",")).map(p => Auction(p(0),p(1).toFloat,p(2).toFloat,p(3),p(4).toInt,p(5).toFloat,p(6).toFloat,p(7),p(8).toInt ))
// Return the first element in this RDD

ebay.first()
//res7: Auction = Auction(8213034705,95.0,2.927373,jake7870,0,95.0,117.5,xbox,3)
// Return the number of elements in the RDD
ebay.toDF().registerTempTable("ebaytable")
sqlContext.sql("select * from ebaytable where price>10 ").show()
// change ebay RDD of Auction objects to a DataFrame
//val auctionDF = ebay.toDF()
//val auction = sparkSession.read.text("ebay.txt").map(_.split(",")).map(p =>
//Auction(p(0),p(1).toFloat,p(2).toFloat,p(3),p(4).toInt,p(5).toFloat,p(6).toFloat,p(7),p(8).toInt )).toDF()
// Display the top 20 rows of DataFrame
//auction.show()
//auction.printSchema()
// How many auctions were held?
//auction.select("auctionid").distinct.count
// Long = 627

// How many bids per item?
//auction.groupBy("auctionid", "item").count.show

// Get the auctions with closing price > 100
//val highprice= auction.filter("price > 100")
// highprice: org.apache.spark.sql.DataFrame = [auctionid: string, bid: float, bidtime: float, bidder: // string, bidderrate: int, openbid: float, price: float, item: string, daystolive: int]

// display dataframe in a tabular format
//highprice.show()

// auctionid  bid   bidtime  bidder         bidderrate openbid price item daystolive
// 8213034705 95.0  2.927373 jake7870       0          95.0    117.5 xbox 3        
// 8213034705 115.0 2.943484 davidbresler2  1          95.0    117.5 xbox 3


// register the DataFrame as a temp table 
//auction.registerTempTable("auction")

// SQL statements can be run 
// How many  bids per auction?
//val results =sqlContext.sql("SELECT auctionid, item,  count(bid) FROM auction GROUP BY auctionid, item")

// display dataframe in a tabular format
//results.show()
// auctionid  item    count
// 3016429446 palm    10
// 8211851222 xbox    28. . . 

//val results2 =sqlContext.sql("SELECT auctionid, MAX(price) FROM auction  GROUP BY item,auctionid")
//results2.show()
// auctionid  c1
// 3019326300 207.5
// 8213060420 120.0 . . . 

  }

}