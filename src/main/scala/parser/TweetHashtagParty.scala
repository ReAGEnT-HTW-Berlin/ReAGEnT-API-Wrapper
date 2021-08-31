package parser

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.bson.Document

// saves the values to a document, which gets saved in mongoDB later
case class TweetHashtagParty (
                          _id: java.util.Map[String, Any],
                          count: Long
                        ) extends Parser {

  def toDocument: Document = {
    val m: java.util.Map[String, Object] = new java.util.HashMap()
    m.put("_id", _id)
    m.put("count", count.asInstanceOf[Object])
    new Document(m)
  }
}

// parses the metrics from dataFrame to values
case class TweetHashtagPartyParser() extends Converter {

  def rowToParser(row: Row): TweetHashtagParty = {
    val startTime = row.getValuesMap(Seq("window")).asInstanceOf[Map[String, Any]].values.toList.head.asInstanceOf[GenericRowWithSchema].getTimestamp(0).toLocalDateTime
    val hashtag = row.getValuesMap(Seq("hashtag")).asInstanceOf[Map[String, Any]].values.toList.head.asInstanceOf[String]
    val party = row.getValuesMap(Seq("party")).asInstanceOf[Map[String, Any]].values.toList.head.asInstanceOf[String]
    val count = row.getValuesMap(Seq("count")).asInstanceOf[Map[String, Any]].values.toList.head.asInstanceOf[Long]

    val id: java.util.Map[String, Any] = new java.util.HashMap()
    id.put("year", startTime.getYear)
    id.put("month", startTime.getMonthValue)
    id.put("day", startTime.getDayOfMonth)
    id.put("hour", startTime.getHour)
    id.put("hashtag", hashtag)
    id.put("party", party)

    TweetHashtagParty(id, count)
  }
}