package twitter

import org.apache.spark.sql.Row
import org.bson.Document

case class TweetLast (
                        _id: String,
                        tweetId: String
                      ) extends Parser {

  def toDocument: Document = {
    val m: java.util.Map[String, Object] = new java.util.HashMap()
    m.put("_id", _id)
    m.put("tweetId", tweetId)
    new Document(m)
  }
}

case class TweetLastParser() extends Converter {

  def rowToParser(row: Row): TweetLast = {
    val party = row.getValuesMap(Seq("party")).asInstanceOf[Map[String, Any]].values.toList.head.asInstanceOf[String]
    val lastTweedId = row.getValuesMap(Seq("id")).asInstanceOf[Map[String, Any]].values.toList.head.asInstanceOf[String]

    TweetLast(party, lastTweedId)
  }
}