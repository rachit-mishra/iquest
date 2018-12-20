package rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.joda.time.LocalDate
import org.joda.time.LocalTime
import org.joda.time.format.DateTimeFormat

object RddExample {
  
  case class Flight (date: LocalDate,
                    airline: String,
                    flightnum : String,
                    origin: String,
                    destination: String,
                    departure: LocalTime,
                    depDelay: Double,
                    arrival: LocalTime,
                    arrivalDelay: Double,
                    airtime: Double,
                    distance: Double);
  
      val spark = SparkSession
          .builder
          .master("local")
          .appName("MyerrorsTesing-app")
          .config("spark.sql.warehouse.dir", "/Users/ravi")
          .getOrCreate()
     
      val sc = spark.sparkContext
    
      def main (args : Array[String]) {
        
      System.setProperty("hadoop.home.dir", "/Users/ravi");
      
      // RDD's can be created in two ways
      // 1. Read data from file. 
      val weatherData =sc.textFile("/Users/ravi/data/weather.txt")
      
      // 2. Create Rdd from an Array
      val numbers = Array(1,2,3,4,5)
      val numberRdd = sc.parallelize(numbers);
      
      // Transformations are lazily evaluated and they are executed when an action is performed on RDD
      // RDD are Resilient distributed datasets
      // Resilient: Fault tolerant 
      // Distributed: Across the Cluster so that we can work on them in parallel
      // RDDs have three important characteristics
      // 1. Partitions: Data is partitioned across the cluster
      // 2. Read only: Immutable. Once created you can read data from them or transform to another RDD
      // 3. Lineage: RDDs are aware of their lineage
      // 
      
      
      
      // Important Rdds are filter, map, flatmap
      
      // filter function is applied on all records within RDD 
      // the matching records will be added to filteredWeatherData
      // In the below examples all records that doesn't contain word Description will be 
      // added to new rdd. 
      val filteredWeatherData = weatherData.filter(text => !text.toString().contains("Description"))
      
      // betweenDates method is applied for every record. 
      val filteredWeatherData1 = weatherData.filter(betweenDates)
      
      // filter takes any function that returns boolean
      
      /* ************************** */
      
      // Map applies a function on each record
      // mapRdd contains the lenght of each record from weatherData
      val mapRdd = weatherData.map(len)
      
      // filter takes any function that returns a single object
      
      /* *************************** */
      
      // flatMap takes each record and converts into multiple records
      // Ex: From RDD of lines of text to and RDD of words
      val flatMapRdd = weatherData.flatMap(text => text.split(','))
      
      // flatMap takes any function that returns Iterator
      
      /* *************************** */
      
      // Some transformations acts on two Rdds to make third
      // For ex: union, intersection, subtract, cartesian 
      
      
      // Some of the actions
      // For ex: take(10), first, collect, reduce, aggregate, count, countByValue
      // 1. take: returns 10 records
      // 2. first: gets first record
      // 3. collect: get all records in rdd
      // 4. reduce:
      // 5. aggregate
      // 6. count: Total number of rows
      // 7. countByValue: Counts number of times a value appears
      
      val flightData =sc.textFile("/Users/ravi/data/flights.txt")
      // In this example we are parsing the record
      val parsedFlights = flightData.map(parse);
      // In this example we are getting distabce and applying reduce function to calculate total distance
      val totalDistance = parsedFlights.map(_.distance).reduce((x,y) => x+y)
      val avgDistance = totalDistance / parsedFlights.count;
      
      // Percentage of flights 
      val delayedFlightPercent = parsedFlights.filter(_.depDelay>0).count.toDouble/parsedFlights.count.toDouble
      
      // Get hourly distribution 
      val hourly = parsedFlights.map(x => (x.depDelay/60).toInt).countByValue()
      
      // Pair RDDs : keys, values, mapValues, groupByKeys, reduceByKey, combineByKey
      // reduceByKey is same as reduce
      // combineByKey is same as aggregate
      // aggregate is called currying in scala. 
      
      // airport delays
      // In this example, we have created a pair rdd by providing two points of interest
      val airportDelays = parsedFlights.map(x => (x.origin, x.depDelay))
      
      airportDelays.keys.take(10);
      
      airportDelays.values.take(10);
      
      // In this case, we are adding x & y 
      val airportTotalDelays = airportDelays.reduceByKey((x,y) => (x+y))
      
      // In this case, we are adding the no of flights per each airport origin
      // Here mapValues will retain the key as same while the value is changed to 1 
      // instead of delay. After that conversion we are applying reduceByKey to add the values by key
      val airportFlightCount = airportDelays.mapValues(x => 1).reduceByKey((x,y) => (x+y))
      
      val airportDelaysFlightCountTuple = airportTotalDelays.join(airportFlightCount)
      
      val airportAvgDelay = airportDelaysFlightCountTuple.mapValues(x => x._1 / x._2.toDouble)
      
      //combineByKey takes three parameters
      // 1. createCombiner
      // 2. merge
      // 3. mergeCombiner
      
      val airportAvgDelayCombineByKey =  airportDelays.combineByKey(value => (value , 1), 
                    (acc: (Double, Int), value) => (acc._1 + value, acc._2 + 1), 
                    (acc1 : (Double, Int), acc2 :(Double, Int)) => (acc1._1+acc2._1, acc1._2 + acc2._2  ) ) 
                    
      
      val airportAvgSort =  airportAvgDelay.sortBy(-_._2).take(10)
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
    }
    
    val errorCount = sc.accumulator(0)
    
    def processLog(line : String) : String = {
      if (line.contains("ERROR")) {
        errorCount+=1
      }
      ""
    }
    
    def betweenDates(line : String): Boolean = {
      true
    }
    
    def len(line : String): Int = {
      line.length()
    }
    
    def parse(row: String): Flight = {
      val fields = row.split(",");
      val datePattern = DateTimeFormat.forPattern("YYYY-mm-dd");
      val date:LocalDate = datePattern.parseDateTime(fields(0)).toLocalDate();
      
      //Build the flight object
      /*
       date: LocalDate,
                    airline: String,
                    flightnum : String,
                    origin: String,
                    destination: String,
                    departure: LocalTime,
                    depDelay: Double,
                    arrival: LocalTime,
                    arrivalDelay: Double,
                    airtime: Double,
                    distance: Double
       */
      Flight(null,"","","","",null,0,null,0,200.0,2000.0);
    }

}