package reagent.mongo

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.{ForeachWriter, _}
import org.bson.Document
import org.mongodb.scala.model.Filters._
import reagent.parser.Converter

import scala.collection.mutable

class MongoUpsertWriter(converter: Converter, dbName: String, collection: String, user: String, pwd: String) extends ForeachWriter[Row] {

  val writeConfig: WriteConfig = WriteConfig(Map("uri" -> s"mongodb://$user:$pwd@localhost:27017/$dbName.$collection?authSource=$dbName"))
  var mongoConnector: MongoConnector = _
  var tweetList: mutable.ArrayBuffer[Row] = _

  override def process(value: Row): Unit = {
    try {
      if (value != null) {
        val options = new ReplaceOptions().upsert(true)
        // update entries in reagent.mongo
        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
          collection.replaceOne(
            equal("_id", converter.rowToParser(value).toDocument.get("_id")),
            converter.rowToParser(value).toDocument,
            options
          )
        })
      }
    } catch {
      case e: Throwable => println("Mongo Exception:" + e.toString)
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