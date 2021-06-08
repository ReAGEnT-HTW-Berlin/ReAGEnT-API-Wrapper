package twitter

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.{ForeachWriter, _}
import org.bson.Document
import utils.TweetWriteMongoConnection

import scala.collection.JavaConverters._
import scala.collection.mutable

class MongoForEachWriter(dbName: String, user: String, pwd: String) extends ForeachWriter[Row] {

  val writeConfig: WriteConfig = WriteConfig(Map("uri" -> s"mongodb://$user:$pwd@141.45.146.253:27017/$dbName.tweets?authSource=$dbName"))
//  val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://reagent:reagent123#!@localhost/TweetDB.tweets"))
  var mongoConnector: MongoConnector = _
  var tweetList: mutable.ArrayBuffer[Row] = _
  //val schema= ScalaReflection.schemaFor[MyTweet].dataType.asInstanceOf[StructType]
  //val encoder = RowEncoder(schema)

  override def process(value: Row): Unit = {
    new TweetWriteMongoConnection(dbName, user, pwd, "bson", "json").writeTweetToMongoDB(value.getString(0), value.getString(11))
    tweetList.append(value)
  }

  override def close(errorOrNull: Throwable): Unit = {
    try {
      if (tweetList.nonEmpty) {
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
