package twitter

import parser.{TweetCountParser, TweetHashtagParser, TweetHashtagPartyParser, TweetLastParser, TweetMediaCountParser, TweetSentimentParser, TweetSourceParser, TweetUserPartyCountParser}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import utils.{MongoForEachWriter, MongoUpdateWriter, MongoUpsertWriter}

import scala.io.BufferedSource

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

    // WORKER THREAD 1
    //   get all tweets from DataFrame and write them to mongoDB
    val tweets: StreamingQuery = tweetDF
      .writeStream
      .foreach(new MongoForEachWriter(dbName, user, pwd))
      .outputMode("append")
      .start()

    // WORKER THREAD 2
    //    get last tweet, sorted by party in parser
    val lastTweets: StreamingQuery = tweetDF.select("party", "id")
      .writeStream
      .foreach(new MongoUpdateWriter(TweetLastParser(), dbName, "lastTweets", user, pwd))
      .outputMode("append")
      .start()

    // WORKER THREAD 3
    //    get hashtags per hour in sink
    val hashtags = tweetDF.select("hashtags")
      .withColumn("timestamp", current_timestamp())
      .withColumn("hashtag", explode($"hashtags"))
      .groupBy(window($"timestamp", "1 hour"), $"hashtag").count()
    //    update into mongodb every minute
    val x: StreamingQuery = hashtags.writeStream
      .trigger(Trigger.ProcessingTime("1 minute"))
      .outputMode("complete")
      .foreach(new MongoUpsertWriter(TweetHashtagParser(), dbName, "hashtagsByHour", user, pwd))
      .start()

    // WORKER THREAD 4
    //    get hashtags per hour and party in sink
    val hashtags_party = tweetDF.select("hashtags", "party").
      withColumn("timestamp", current_timestamp()).
      withColumn("hashtag", explode($"hashtags")).
      groupBy(window($"timestamp", "1 hour"), $"hashtag", $"party").count()
    //    update into mongodb every minute
    val y: StreamingQuery = hashtags_party.writeStream
      .trigger(Trigger.ProcessingTime("1 minute"))
      .outputMode("complete")
      .foreach(new MongoUpsertWriter(TweetHashtagPartyParser(), dbName, "hashtagsByHourAndParty", user, pwd))
      .start()

    // WORKER THREAD 5
    //    get tweet count per hour and party in sink
    val windowedCounts_1h = tweetDF.select("party").
      withColumn("timestamp", current_timestamp()).
      groupBy(window($"timestamp", "1 hour"), $"party").count()
    //    update into mongodb every minute
    val z: StreamingQuery = windowedCounts_1h.writeStream
      .trigger(Trigger.ProcessingTime("1 minute"))
      .outputMode("complete")
      .foreach(new MongoUpsertWriter(TweetCountParser(), dbName, "metricsByHourAndParty", user, pwd))
      .start()

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

    while (running) {}

    println("**********************")
    println(" Spark Thread stopped ")
    println("**********************")
    tweets.stop
    lastTweets.stop
    x.stop
    y.stop
    z.stop
    mediaUsageWriter.stop
    mostActiveWriter.stop
    sentimentWriter.stop
    sourceWriter.stop

    TwitterConnectionImpl.stop

    spark.sparkContext.cancelAllJobs()
    spark.sparkContext.stop()

    spark.close()
    spark.stop

    tweetDF.sparkSession.close()
    tweetDF.sparkSession.stop()

    tweetDF.sparkSession.streams.awaitAnyTermination()
    spark.streams.awaitAnyTermination()
  }

  /*
      read in database name user and password
   */
  def setConfig(): Unit = {
    val bufSource: BufferedSource = scala.io.Source.fromFile("/usr/share/reagent-api-wrapper/config.txt")
    val bufReader = bufSource.bufferedReader
    try {
      dbName = bufReader.readLine
      user = bufReader.readLine
      pwd = bufReader.readLine
    } catch {
      case e: Exception => println("wrong config.txt - write dbName, user, password line after line")
    }
  }

  def stop(): Unit = {
    running = false
  }
}