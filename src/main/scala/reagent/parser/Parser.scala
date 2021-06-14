package reagent.parser

import org.apache.spark.sql.Row
import org.bson.Document

// to make method available in UpsertWriter
abstract class Parser() {
  def toDocument() : Document
}

// to make method available in UpsertWriter
abstract class Converter() {
  def rowToParser(row: Row) : Parser
}

