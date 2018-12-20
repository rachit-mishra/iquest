package recomandation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.mllib.recommendation.{ ALS,MatrixFactorizationModel, Rating}

//https://www.mapr.com/ebooks/spark/08-recommendation-engine-spark.html

object MovieALS_1 {

  // input format MovieID::Title::Genres
  case class Movie(movieId: Int, title: String, genres: Seq[String])
  // input format is UserID::Gender::Age::Occupation::Zip-code
  case class User(userId: Int, gender: String, age: Int,
                  occupation: Int, zip: String)


  def main(args: Array[String]) {
      System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
     //session
     val sparkSession = SparkSession.builder
                 .master("local")
                 .config("spark.sql.warehouse.dir", "file:///C:\\Users\\Ravee\\IdeaProjects\\Testing\\spark-warehouse")
                 .appName("my-spark-app")
                 .getOrCreate()
    val sc =  sparkSession.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkSession.sparkContext)
    import sqlContext.implicits._

    val ratingText = sc.textFile("data/ratings.dat.txt")
    ratingText.first()
    
    
    val ratingsRDD = ratingText.map(parseRating).cache()
    println("Total number of ratings: " + ratingsRDD.count())
    println("Total number of movies rated: " +
      ratingsRDD.map(_.product).distinct().count())
    println("Total number of users who rated movies: " +
      ratingsRDD.map(_.user).distinct().count())

    //load the data into DataFrames
    val usersDF = sc.textFile("data/users.dat.txt").map(parseUser).toDF()
    val moviesDF = sc.textFile("data/movies.dat.txt"). map(parseMovie).toDF()
    val ratingsDF = ratingsRDD.toDF()
    
    ratingsDF.registerTempTable("ratings")
    moviesDF.registerTempTable("movies")
    usersDF.registerTempTable("users")
    //usersDF.printSchema()
    //moviesDF.printSchema()
    //ratingsDF.printSchema()

    // Get the max, min ratings along with the count of users who have
    // rated a movie.
    val results = sqlContext.sql(
      """select movies.title,
         movierates.maxr,
         movierates.minr, movierates.cntu
    from(
        SELECT ratings.product,
          max(ratings.rating) as maxr,
          min(ratings.rating) as minr,
          count(distinct user) as cntu
        FROM ratings group by ratings.product ) movierates
          join movies on movierates.product=movies.movieId
        order by movierates.cntu desc"""
    )
    //results.show()

    // Show the top 10 most-active users and how many times they rated a movie
    val mostActiveUsersSchemaRDD = sqlContext.sql(
      """SELECT ratings.user, count(*) as ct from ratings
      group by ratings.user order by ct desc limit 10""")
     //println(mostActiveUsersSchemaRDD.collect().mkString("\n"))

    // Find the movies that user 4169 rated higher than 4
    val results1 = sqlContext.sql("""SELECT ratings.user, ratings.product,
            ratings.rating, movies.title FROM ratings JOIN movies
            ON movies.movieId=ratings.product
            where ratings.user=4169 and ratings.rating > 4""")
    //results1.show

    // Randomly split ratings RDD into training
    // data RDD (80%) and test data RDD (20%)
    val splits = ratingsRDD.randomSplit(Array(0.8, 0.2), 0L)
    val trainingRatings = splits(0).cache()
    val testRatings = splits(1).cache()

    val numTraining = trainingRatings.count()
    val numTest = testRatings.count()
   // println(s"Training: $numTraining, test: $numTest.")

    //train the model
    val model = new ALS().setRank(20).setIterations(10).run(trainingRatings)
    //Get the top 4 movie predictions for user 4169
    val topRecsForUser = model.recommendProducts(4169, 5)
    // get movie titles to show with recommendations
    val topRecsForUserDF= sc.parallelize(topRecsForUser).toDF()
    //topRecsForUserDF.show()
    topRecsForUserDF.registerTempTable("topRecsForUserDFTBL")
    val topMovies = sparkSession.sql("" +
      " select a.user," +
      " a.product, a.rating,b.title " +
      " from topRecsForUserDFTBL a, movies b" +
      " where a.product =b.movieId order by a.product")
    //topMovies.show()

       //Predict from test data./////////////////////////////////////

       // get user product pair from testRatings
        val testUserProductRDD = testRatings.map {
          case Rating(user, product, rating) => (user, product)
       }

    // get predicted ratings to compare to test ratings
        val predictionsForTestRDD  = model.predict(testUserProductRDD)

    testRatings.toDF().registerTempTable("testRatings")
    predictionsForTestRDD.toDF().registerTempTable("predictionsKeyedByUserProduct")
    //testRatings.toDF().printSchema()
    //predictionsForTestRDD.toDF().printSchema()
    //predictionsForTestRDD.toDF().show()
   val testAndPredictionsJoined = sparkSession.sql("select a.user,a.product," +
     " a.rating as actualRating, b.rating as PredictedRating" +
     " from testRatings a, predictionsKeyedByUserProduct b" +
     " where a.user = b.user" +
     " and a.product = b.product")
    //testAndPredictionsJoined.show()
    testAndPredictionsJoined.registerTempTable("falsePositivesTbl")

        val falsePositives = sparkSession.sql("select user, product, actualRating," +
          " PredictedRating from falsePositivesTbl" +
          " where  actualRating <= 1 and  PredictedRating >=4")
        falsePositives.show()
        falsePositives.count()
        println("falsePositives.count():"+falsePositives.count())

    sc.stop();
  }

  // function to parse input into Movie class
  def parseMovie(str: String): Movie = {
    val fields = str.split("::")
    assert(fields.size == 3)
    Movie(fields(0).toInt, fields(1), Seq(fields(2)))
  }

  // function to parse input into User class
  def parseUser(str: String): User = {
    val fields = str.split("::")
    assert(fields.size == 5)
    User(fields(0).toInt, fields(1).toString, fields(2).toInt,
      fields(3).toInt, fields(4).toString)
  }

  def parseRating(str: String): Rating= {
    val fields = str.split("::")
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
  }

}