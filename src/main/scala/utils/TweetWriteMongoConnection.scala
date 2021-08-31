package utils

import org.mongodb.scala._

class TweetWriteMongoConnection(dbName:String, user:String, pwd:String, collBSON: String, collJSON: String) {

  val connectionString: String = s"mongodb://${user}:${pwd}@141.45.146.253:27017/?authSource=${dbName}"
  val mongoClient: MongoClient = MongoClient(connectionString)
  val database: MongoDatabase = mongoClient.getDatabase(dbName)
  val cBSON: MongoCollection[Document] = database.getCollection(collBSON)
  val cJSON: MongoCollection[Document] = database.getCollection(collJSON)

  def writeTweetToMongoDB(tweetID: String, tweetJSON: String) = {

    val b= cBSON.insertOne(Document(tweetJSON))
    b.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = {}//println("BSON-Tweet "+tweetID+" inserted")}
      override def onError(e: Throwable): Unit = println("BSON: Cannot insert tweet: "+ tweetJSON)
      override def onComplete(): Unit = {}
    })

    val jsonDoc = Document("id" -> tweetID, "tweet" -> tweetJSON)
    val c= cJSON.insertOne(jsonDoc)
    c.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = {}//println("JSON-Tweet "+tweetID+" inserted")}
      override def onError(e: Throwable): Unit = println("JSON: Cannot insert tweet: "+ tweetJSON)
      override def onComplete(): Unit = {}
    })
  }

  def closeMongoConnection= {
    Thread.sleep(1 * 1000)
    mongoClient.close
  }
}
