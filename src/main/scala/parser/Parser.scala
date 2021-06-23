package parser

import org.apache.spark.sql.Row
import org.bson.Document

abstract class Parser() {
  def toDocument() : Document
}

abstract class Converter() {
  def rowToParser(row: Row) : Parser
}

