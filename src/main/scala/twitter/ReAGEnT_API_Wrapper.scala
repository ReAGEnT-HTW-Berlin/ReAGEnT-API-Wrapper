package twitter

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.BufferedSource

object ReAGEnT_API_Wrapper {
  private val SOURCE_PROVIDER_CLASS = TwitterStreamingSource.getClass.getCanonicalName
  var dbName: String = "examples"
  var user: String = "seija"
  var pwd: String = "RhoMTXB2"

  def main(args: Array[String]): Unit = {
//    setConfig()

    println("ReAGEnt_API_Wrapper")

    val providerClassName = SOURCE_PROVIDER_CLASS.substring(0, SOURCE_PROVIDER_CLASS.indexOf("$"))
    println(providerClassName)

    val spark = SparkSession
      .builder
      .appName("ReAGEnt_API_Wrapper")
      .master("local[*]")
      .getOrCreate()

    val bearerToken = sys.env.getOrElse("TWITTER_BEARER", "")
    println("BEARER: " + bearerToken)
    println("dbName: " + dbName)
    println("user: " + user)
    println("pwd: " + pwd)

    val tweetDF: DataFrame = spark.readStream.
      format(providerClassName).
      option(TwitterStreamingSource.QUEUE_SIZE, 10000).

      load

    tweetDF.printSchema

    import spark.implicits._

    // get all tweets and write them to mongoDB
    val tweets: StreamingQuery = tweetDF
      .writeStream
      .foreach(new MongoForEachWriter(dbName, user, pwd))
      .outputMode("append")
      .start()

    //    get last tweet, sorted by party in parser
    val lastTweets: StreamingQuery = tweetDF.select("party", "id")
      .writeStream
      .foreach(new MongoUpsertWriter(TweetLastParser(), dbName, "lastTweets", user, pwd))
      .outputMode("append")
      .start()

    //    get hashtags per hour in sink
    val hashtags = tweetDF.select("hashtags")
      .withColumn("timestamp", current_timestamp())
      .withColumn("hashtag", explode($"hashtags"))
      .groupBy(window($"timestamp", "1 hour"), $"hashtag").count()
    //    update them into mongodb every minute
    val x: StreamingQuery = hashtags.writeStream
      .trigger(Trigger.ProcessingTime("1 minute"))
      .outputMode("complete")
      .foreach(new MongoUpsertWriter(TweetHashtagParser(), dbName, "hashtagsByHour", user, pwd))
      .start()

    //    get hashtags per hour and party in sink
    val hashtags_party = tweetDF.select("hashtags", "party").
      withColumn("timestamp", current_timestamp()).
      withColumn("hashtag", explode($"hashtags")).
      groupBy(window($"timestamp", "1 hour"), $"hashtag", $"party").count()
    //    update them into mongodb every minute
    val y: StreamingQuery = hashtags_party.writeStream
      .trigger(Trigger.ProcessingTime("1 minute"))
      .outputMode("complete")
      .foreach(new MongoUpsertWriter(TweetHashtagPartyParser(), dbName, "hashtagsByHourAndParty", user, pwd))
      .start()

    //    get tweets per hour and party in sink
    val windowedCounts_1h = tweetDF.select("party").
      withColumn("timestamp", current_timestamp()).
      groupBy(window($"timestamp", "1 hour"), $"party").count()
    //    update them into mongodb every minute
    val z: StreamingQuery = windowedCounts_1h.writeStream
      .trigger(Trigger.ProcessingTime("1 minute"))
      .outputMode("complete")
      .foreach(new MongoUpsertWriter(TweetCountParser(), dbName, "metricsByHourAndParty", user, pwd))
      .start()

    while (true) {}

    println("*******************************************************************************************")
    println("Spark Thread stopped")
    println("*******************************************************************************************")
    tweets.stop
    lastTweets.stop
    x.stop
    y.stop
    z.stop
    TwitterConnectionImpl.stop
    spark.stop

  }

  /*
    read in database name user and password
 */
  def setConfig(): Unit = {
    val bufSource: BufferedSource = scala.io.Source.fromFile("config.txt")
    val bufReader = bufSource.bufferedReader
    try {
      dbName = bufReader.readLine
      user = bufReader.readLine
      pwd = bufReader.readLine
    } catch {
      case e: Exception => println("wrong config.txt - write dbName, user, password line after line")
    }
  }
}