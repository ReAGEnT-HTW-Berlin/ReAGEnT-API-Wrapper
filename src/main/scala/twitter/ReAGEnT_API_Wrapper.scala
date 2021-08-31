package twitter

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import parser._
import utils.{MongoForEachWriter, MongoUpdateWriter, MongoUpsertWriter}

import scala.collection.mutable.ArrayBuffer


object ReAGEnT_API_Wrapper {
  private val SOURCE_PROVIDER_CLASS = TwitterStreamingSource.getClass.getCanonicalName
  private var running: Boolean = _
  var dbName: String = sys.env.getOrElse("dbName", "")
  var user: String = sys.env.getOrElse("user", "")
  var pwd: String = sys.env.getOrElse("pwd", "")

  def main(args: Array[String]): Unit = {
    running = true

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
      option(TwitterStreamingSource.QUEUE_SIZE, 1000).
      load
    tweetDF.printSchema

    import spark.implicits._

    var writers = new ArrayBuffer[StreamingQuery]()

    // WORKER THREAD 1
    //   get all tweets from DataFrame and write them to mongoDB
    val tweets: StreamingQuery = tweetDF
      .writeStream
      .foreach(new MongoForEachWriter(dbName, user, pwd)) // writes every tweet to mongodb
      .outputMode("append")
      .start()
    writers += tweets

    // WORKER THREAD 2
    //    get last tweet, sorted by party in parser
    val lastTweets: StreamingQuery = tweetDF.select("party", "id")
      .writeStream
      .foreach(new MongoUpdateWriter(TweetLastParser(), dbName, "lastTweets", user, pwd)) // updates the last 15 tweets as they come to mongodb
      .outputMode("append")
      .start()
    writers += lastTweets

    // WORKER THREAD 3
    //    get hashtags per hour in sink
    val hashtags = tweetDF.select("hashtags")
      .withColumn("timestamp", current_timestamp())
      .withColumn("hashtag", explode($"hashtags"))
      .groupBy(window($"timestamp", "1 hour"), $"hashtag").count()
    //    update into mongodb every minute
    val hashtagsWriter: StreamingQuery = hashtags.writeStream
      .trigger(Trigger.ProcessingTime("1 minute"))
      .outputMode("complete")
      .foreach(new MongoUpsertWriter(TweetHashtagParser(), dbName, "hashtagsByHour", user, pwd)) // upserts the current most used hashtags to mongodb
      .start()
    writers += hashtagsWriter

    // WORKER THREAD 4
    //    get hashtags per hour and party in sink
    val hashtagsParty = tweetDF.select("hashtags", "party").
      withColumn("timestamp", current_timestamp()).
      withColumn("hashtag", explode($"hashtags")).
      groupBy(window($"timestamp", "1 hour"), $"hashtag", $"party").count()
    //    update into mongodb every minute
    val hashtagsPartyWriter: StreamingQuery = hashtagsParty.writeStream
      .trigger(Trigger.ProcessingTime("1 minute"))
      .outputMode("complete")
      .foreach(new MongoUpsertWriter(TweetHashtagPartyParser(), dbName, "hashtagsByHourAndParty", user, pwd))
      .start()
    writers += hashtagsPartyWriter

    // WORKER THREAD 5
    //    get tweet count per hour and party in sink
    val windowedCounts = tweetDF.select("party").
      withColumn("timestamp", current_timestamp()).
      groupBy(window($"timestamp", "1 hour"), $"party").count()
    //    update into mongodb every minute
    val windowedCountsWriter: StreamingQuery = windowedCounts.writeStream
      .trigger(Trigger.ProcessingTime("1 minute"))
      .outputMode("complete")
      .foreach(new MongoUpsertWriter(TweetCountParser(), dbName, "metricsByHourAndParty", user, pwd))
      .start()
    writers += windowedCountsWriter

    // WORKER THREAD 6
    //    get media usage count per hour and party in sink
    val mediaUsage = tweetDF.select("party", "attachments").where("attachments != 0").
      withColumn("timestamp", current_timestamp()).
      groupBy(window($"timestamp", "1 hour"), $"attachments", $"party").count()
    //    update into mongodb every minute
    val mediaUsageWriter: StreamingQuery = mediaUsage.writeStream
      .trigger(Trigger.ProcessingTime("5 minutes"))
      .outputMode("complete")
      .foreach(new MongoUpsertWriter(TweetMediaCountParser(), dbName, "mediaUsageRunningByHourAndParty", user, pwd))
      .start()
    writers += mediaUsageWriter

    // WORKER THREAD 7
    //    get tweet count per user, hour and party in sink
    val mostActive = tweetDF.select("party", "username").
      withColumn("timestamp", current_timestamp()).
      groupBy(window($"timestamp", "1 hour"), $"username", $"party").count()
    //    update into mongodb every minute
    val mostActiveWriter: StreamingQuery = mostActive.writeStream
      .trigger(Trigger.ProcessingTime("5 minutes"))
      .outputMode("complete")
      .foreach(new MongoUpsertWriter(TweetUserPartyCountParser(), dbName, "mostActiveRunningByHourAndParty", user, pwd))
      .start()
    writers += mostActiveWriter

    // WORKER THREAD 8
    //    get average tweet sentiment per hour and party in sink
    val sentiment = tweetDF.select("party", "sentiment").
      withColumn("timestamp", current_timestamp()).
      groupBy(window($"timestamp", "1 hour"), $"party").avg("sentiment")
    //    update into mongodb every 5 minutes
    val sentimentWriter: StreamingQuery = sentiment.writeStream
      .trigger(Trigger.ProcessingTime("5 minutes"))
      .outputMode("complete")
      .foreach(new MongoUpsertWriter(TweetSentimentParser(), dbName, "sentimentRunningByHourAndParty", user, pwd))
      .start()
    writers += sentimentWriter

    // WORKER THREAD 9
    //   get source device usage per hour and party in sink
    val source = tweetDF.select("party", "source").
      withColumn("timestamp", current_timestamp()).
      groupBy(window($"timestamp", "1 hour"), $"party", $"source").count()
    //    update into mongodb every 5 minutes
    val sourceWriter: StreamingQuery = source.writeStream
      .trigger(Trigger.ProcessingTime("5 minutes"))
      .outputMode("complete")
      .foreach(new MongoUpsertWriter(TweetSourceParser(), dbName, "sourceRunningByHourAndParty", user, pwd))
      .start()
    writers += sourceWriter

    while (running) {} // gets interrupted when error is parsed in MyTweet -> restarts application

    // stop all writers
    for (writer <- writers)
      writer.stop

    // stop connection to twitter (important because of connection limitations)
    TwitterConnectionImpl.stop

    // shut down spark
    spark.sparkContext.cancelAllJobs
    spark.sparkContext.stop
    spark.close
    spark.stop
    tweetDF.sparkSession.close
    tweetDF.sparkSession.stop
    tweetDF.sparkSession.streams.awaitAnyTermination
    spark.streams.awaitAnyTermination

    println("(!) leaving API Wrapper . . .")
    sys.exit(121) // signal systemd to restart the application
  }

  def stop = {
    running = false
  }
}