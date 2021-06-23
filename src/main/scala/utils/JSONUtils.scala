package utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
 * Provides methods for json object serialization and deserialization.
 * Currently uses the jackson library to accomplish this and acts as a wrapper around it.
 *
 * jackson is a dependency of spark and also used internally by spark
 *
 */
object JSONUtils {
  private val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  /**
   * Attempts to converts the given object to json.
   * Throws an exception if something goes wrong.
   *
   * @param obj the object
   * @return object as json string
   *
   */
  def toJson[T](obj: T): String =
    mapper.writer().writeValueAsString(obj)

  /**
   * Attempts to read a json string into a object.
   * Throws an exception if something goes wrong
   *
   * @param json the json string
   * @return the object
   */
  def fromJson[T](json: String)(implicit m: Manifest[T]): T = mapper.readValue[T](json)

  /**
   * Attempts to parse a json object into a Map.
   * If there is an error, None is returned instead.
   *
   * @example {"key":"v1", "key2":{"innerKey":"v2"}} would result in the following scala Map
   *          Map("key"->"v1","key2"->Map("innerKey","v2"))
   * @param json the json string
   * @return Some(Map) or None if there was a problem parsing the json string
   *
   */
  def parseJson(json: String): Option[Map[String, Any]] = try {
    Some(mapper.readValue[Map[String, Any]](json))
  } catch {
    case e: Exception => None
  }

}
