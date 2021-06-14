package reagent

import org.apache.spark.sql.functions.{current_timestamp, explode, window}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import reagent.mongo.{MongoForEachWriter, MongoUpsertWriter}
import reagent.parser.{TweetCountParser, TweetHashtagParser, TweetHashtagPartyParser, TweetLastParser}
import reagent.twitter.{TwitterConnectionImpl, TwitterStreamingSource}

import scala.io.BufferedSource

object ReAGEnT_API_Wrapper {
  private val SOURCE_PROVIDER_CLASS = TwitterStreamingSource.getClass.getCanonicalName
  private var running: Boolean = _
  var dbName: String = _
  var user: String = _
  var pwd: String = _

  def main(args: Array[String]): Unit = {
    running = true
    setConfig()

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
      .foreach(new MongoUpsertWriter(TweetLastParser(), dbName, "lastTweets", user, pwd))
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

    while (running) {}

    println("**********************")
    println(" Spark Thread stopped ")
    println("**********************")
    tweets.stop
    lastTweets.stop
    x.stop
    y.stop
    z.stop

    TwitterConnectionImpl.stop

    spark.sparkContext.cancelAllJobs()
    spark.sparkContext.stop()

    spark.close()
    spark.stop

    tweetDF.sparkSession.close()
    tweetDF.sparkSession.stop()

    // wait for spark to close all streams, so the workers can save their data
    tweetDF.sparkSession.streams.awaitAnyTermination()
    spark.streams.awaitAnyTermination()

    // exit the application so systemd restarts it
    System.exit(121)
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
