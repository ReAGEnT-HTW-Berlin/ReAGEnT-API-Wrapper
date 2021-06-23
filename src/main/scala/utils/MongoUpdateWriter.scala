package utils

import java.util

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.{ForeachWriter, _}
import org.bson.Document
import org.mongodb.scala.model.Filters._
import parser.{Converter, TweetLast}

import scala.collection.mutable

class MongoUpdateWriter(converter: Converter, dbName: String, collection: String, user: String, pwd: String) extends ForeachWriter[Row] {

  val writeConfig: WriteConfig = WriteConfig(Map("uri" -> s"mongodb://$user:$pwd@141.45.146.253:27017/$dbName.$collection?authSource=$dbName"))
  var mongoConnector: MongoConnector = _
  var tweetList: mutable.ArrayBuffer[Row] = _

  override def process(value: Row): Unit = {
    try {
      if (value != null) {
        val options = new ReplaceOptions().upsert(true)

        // combined
        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] => {
          val currentIt = collection.find(equal("_id", "combined"))
          var current: Document = TweetLast("combined", new util.ArrayList[String]()).toDocument

          current = currentIt.first()

          val currentTweets = current.getOrDefault("tweetIds", new util.ArrayList[String]()).asInstanceOf[util.ArrayList[String]]
          val newTweet = value.getValuesMap(Seq("id")).asInstanceOf[Map[String, Any]].values.toList.head.asInstanceOf[String]

          if (currentTweets.size < 15)
            currentTweets.add(newTweet)
          if (currentTweets.size == 15) {
            currentTweets.remove(0)
            currentTweets.add(newTweet)
          }

          val result = collection.replaceOne(
            equal("_id", "combined"),
            TweetLast(current.get("_id").toString, currentTweets).toDocument,
            options
          )
          result
        }
        })

        // by Party
        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] => {
          val currentIt = collection.find(equal("_id", converter.rowToParser(value).toDocument.get("_id")))
          var current: Document = TweetLast(converter.rowToParser(value).toDocument.get("_id").toString, new util.ArrayList[String]()).toDocument

          current = currentIt.first()


          val currentTweets = current.getOrDefault("tweetIds", new util.ArrayList[String]()).asInstanceOf[util.ArrayList[String]]
          val newTweet = value.getValuesMap(Seq("id")).asInstanceOf[Map[String, Any]].values.toList.head.asInstanceOf[String]

          if (currentTweets.size < 15)
            currentTweets.add(newTweet)
          if (currentTweets.size == 15) {
            currentTweets.remove(0)
            currentTweets.add(newTweet)
          }

          val result = collection.replaceOne(
            equal("_id", converter.rowToParser(value).toDocument.get("_id")),
            TweetLast(current.get("_id").toString, currentTweets).toDocument,
            options
          )
          result
        }
        })

      }
    } catch {
      case e: Throwable => println("Mongo Update Exception:" + e.toString)
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    // dont
  }

  override def open(partitionId: Long, version: Long): Boolean = {
    try {
      mongoConnector = MongoConnector(writeConfig.asOptions)
      tweetList = new mutable.ArrayBuffer[Row]()
      true
    } catch {
      case e: Throwable => println("Mongo Exception:" + e.toString); false
    }
  }
}