package parser

import java.util

import org.apache.spark.sql.Row
import org.bson.Document

// saves the values to a document, which gets saved in mongoDB later
case class TweetLast (
                       _id: String,
                       tweetIds: util.ArrayList[String]
                     ) extends Parser {

  def toDocument: Document = {
    val m: java.util.Map[String, Object] = new java.util.HashMap()
    m.put("_id", _id)
    //    val ids = new util.ArrayList[String]
    //    tweetIds.foreach(id => ids.add(id))
    m.put("tweetIds", tweetIds)
    new Document(m)
  }
}

// parses the metrics from dataFrame to values
case class TweetLastParser() extends Converter {

  def rowToParser(row: Row): TweetLast = {
    val party = row.getValuesMap(Seq("party")).asInstanceOf[Map[String, Any]].values.toList.head.asInstanceOf[String]
    //    val lastTweedId = (row.getValuesMap(Seq("id")).asInstanceOf[Map[String, Any]].values.toList.head.asInstanceOf[String])

    TweetLast(party, new util.ArrayList[String]())
  }
}