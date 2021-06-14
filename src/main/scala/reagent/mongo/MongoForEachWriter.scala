package reagent.mongo

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.{ForeachWriter, _}
import org.bson.Document
import reagent.parser.MyTweet
import reagent.utils.TweetWriteMongoConnection

import scala.collection.JavaConverters._
import scala.collection.mutable

class MongoForEachWriter(dbName: String, user: String, pwd: String) extends ForeachWriter[Row] {

  val writeConfig: WriteConfig = WriteConfig(Map("uri" -> s"mongodb://$user:$pwd@localhost:27017/$dbName.tweets?authSource=$dbName"))
  var mongoConnector: MongoConnector = _
  var tweetList: mutable.ArrayBuffer[Row] = _

  override def process(value: Row): Unit = {
    // write tweet to reagent.mongo as is
    new TweetWriteMongoConnection(dbName, user, pwd, "bson", "json").writeTweetToMongoDB(value.getString(0), value.getString(11))
    // save to buffer
    tweetList.append(value)
  }

  override def close(errorOrNull: Throwable): Unit = {
    try {
      if (tweetList.nonEmpty) {
        // write parsed tweet to reagent.mongo
        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
          collection.insertMany(tweetList.map(row => {
            val tweet = MyTweet.createTweetFromRow(row)
            tweet.toDocument}).asJava)
        })
      }
    } catch {
      case e: Throwable => println("Mongo Exception:" + e.toString)
    }
  }

  override def open(partitionId: Long, version: Long): Boolean = {
    try{
    mongoConnector = MongoConnector(writeConfig.asOptions)
    tweetList = new mutable.ArrayBuffer[Row]()
      true
    } catch {
      case e: Throwable => println("Mongo Exception:" + e.toString); false
    }
  }
}
