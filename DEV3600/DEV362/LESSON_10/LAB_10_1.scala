

//  SQLContext entry point for working with structured data
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
// Import Spark SQL data types 
import org.apache.spark.sql._
// Import MLLIB data types 
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

// define the schemas using a case classes
// input format MovieID::Title::Genres
case class Movie(movieId: Int, title: String)

// input format UserID::Gender::Age::Occupation::Zip-code
case class User(userId: Int, gender: String, age: Int, occupation: Int, zip: String)

    // function to parse input into Movie class  
    def parseMovie(str: String): Movie = {
      val fields = str.split("::")
      assert(fields.size == 3)
      Movie(fields(0).toInt, fields(1))
    }

    // function to parse input into User class
    def parseUser(str: String): User = {
      val fields = str.split("::")
      assert(fields.size == 5)
      User(fields(0).toInt, fields(1).toString, fields(2).toInt, fields(3).toInt, fields(4).toString)
    }

    // function to parse input UserID::MovieID::Rating
    // and pass into  constructor for org.apache.spark.mllib.recommendation.Rating class
    def parseRating(str: String): Rating = {
      val fields = str.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }

// load the data into an RDD
val ratingText = sc.textFile("/user/user01/data/ratings.dat")
val ratingsRDD = ratingText.map(parseRating).cache()
// count number of total ratings
val numRatings = ratingsRDD.count()
// count number of users who rated a movie 
val numUsers = ratingsRDD.map(_.user).distinct().count()
// count number of movies rated 
val numMovies = ratingsRDD.map(_.product).distinct().count()
println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")
// load the data into DataFrames
val moviesDF= sc.textFile("/user/user01/data/movies.dat").map(parseMovie).toDF()  
val usersDF = sc.textFile("/user/user01/data/users.dat").map(parseUser).toDF() 
// create a DataFrame from ratingsRDD
val ratingsDF = ratingsRDD.toDF()

ratingsDF.registerTempTable("ratings")
moviesDF.registerTempTable("movies")
usersDF.registerTempTable("users")

ratingsDF.select("product").distinct.count
ratingsDF.groupBy("product", "rating").count.show
ratingsDF.groupBy("product").count.agg(min("count"), avg("count"),max("count")).show
ratingsDF.select("product", "rating").groupBy("product", "rating").count.agg(min("count"), avg("count"),max("count")).show


// Count the max, min ratings along with the number of users who have rated a movie. Display the title, max rating, min rating, number of users. 
val results =sqlContext.sql("select movies.title, movierates.maxr, movierates.minr, movierates.cntu from(SELECT ratings.product, max(ratings.rating) as maxr, min(ratings.rating) as minr,count(distinct user) as cntu FROM ratings group by ratings.product ) movierates join movies on movierates.product=movies.movieId order by movierates.cntu desc ")

// Show the top 10 most-active users and how many times they rated a movie
val mostActiveUsersSchemaRDD = sqlContext.sql("SELECT ratings.user, count(*) as ct from ratings group by ratings.user order by ct desc limit 10")
mostActiveUsersSchemaRDD.take(20).foreach(println)

val results =sqlContext.sql("SELECT ratings.user, ratings.product, ratings.rating, movies.title FROM ratings JOIN movies ON movies.movieId=ratings.product where ratings.user=4169 and ratings.rating > 4 order by ratings.rating desc ")

// Randomly split ratings RDD into training data RDD (80%) and test data RDD (20%)
val splits = ratingsRDD.randomSplit(Array(0.8, 0.2), 0L)

val trainingRatingsRDD = splits(0).cache()
val testRatingsRDD = splits(1).cache()

val numTraining = trainingRatingsRDD.count()
val numTest = testRatingsRDD.count()
println(s"Training: $numTraining, test: $numTest.")

// Build the recommendation model using ALS with rank=20, iterations=10
val model = ALS.train(trainingRatingsRDD, 20, 10)

val model = (new ALS().setRank(20).setIterations(10).run(trainingRatingsRDD))

// Make movie predictions for user 4169 
val topRecsForUser = model.recommendProducts(4169, 10)

// get movie titles to show with recommendations 
val movieTitles=moviesDF.map(array => (array(0), array(1))).collectAsMap()

// print out top recommendations for user 4169 with titles
topRecsForUser.map(rating => (movieTitles(rating.product), rating.rating)).foreach(println)

// get predicted ratings to compare to test ratings
val predictionsForTestRDD  = model.predict(testRatingsRDD.map{case Rating(user, product, rating) => (user, product)})

predictionsForTestRDD.take(10).mkString("\n")

// prepare the predictions for comparison
val predictionsKeyedByUserProductRDD = predictionsForTestRDD.map{ 
  case Rating(user, product, rating) => ((user, product), rating)
}
// prepare the test for comparison
val testKeyedByUserProductRDD = testRatingsRDD.map{ 
  case Rating(user, product, rating) => ((user, product), rating) 
}

//Join the test with the predictions
val testAndPredictionsJoinedRDD = testKeyedByUserProductRDD.join(predictionsKeyedByUserProductRDD)

testAndPredictionsJoinedRDD.take(10).mkString("\n")

val falsePositives =(testAndPredictionsJoinedRDD.filter{
  case ((user, product), (ratingT, ratingP)) => (ratingT <= 1 && ratingP >=4) 
  })

//Evaluate the model using Mean Absolute Error (MAE) between test and predictions 
val meanAbsoluteError = testAndPredictionsJoinedRDD.map { 
  case ((user, product), (testRating, predRating)) => 
    val err = (testRating - predRating)
    Math.abs(err)
}.mean()

