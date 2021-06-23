package parser

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.bson.Document

import scala.util.Try

// saves the values to a document, which gets saved in mongoDB later
case class TweetMediaCount(
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
case class TweetMediaCountParser() extends Converter {

  def rowToParser(row: Row): TweetMediaCount = {
    val startTime = row.getValuesMap(Seq("window")).asInstanceOf[Map[String, Any]].values.toList.head.asInstanceOf[GenericRowWithSchema].getTimestamp(0).toLocalDateTime
    val party = row.getValuesMap(Seq("party")).asInstanceOf[Map[String, Any]].values.toList.head.asInstanceOf[String]
    val count = row.getValuesMap(Seq("count")).asInstanceOf[Map[String, Any]].values.toList.head.asInstanceOf[Long]
    val media = Try(row.getValuesMap(Seq("attachments")).asInstanceOf[Map[String, Any]].values.toList.head.toString.toLong).getOrElse(0L)

    val id: java.util.Map[String, Any] = new java.util.HashMap()
    id.put("year", startTime.getYear)
    id.put("month", startTime.getMonthValue)
    id.put("day", startTime.getDayOfMonth)
    id.put("hour", startTime.getHour)
    id.put("party", party)

    TweetMediaCount(id, count * media)
  }
}