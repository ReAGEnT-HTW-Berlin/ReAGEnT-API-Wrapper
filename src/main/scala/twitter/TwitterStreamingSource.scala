package twitter

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.types._
import java.util.Optional

class TwitterStreamingSource extends DataSourceV2 with MicroBatchReadSupport with DataSourceRegister with Logging {

  override def createMicroBatchReader(
                                       schema: Optional[StructType],
                                       checkpointLocation: String,
                                       options: DataSourceOptions): MicroBatchReader = {
    new TwitterStreamMicroBatchReader(options)
  }

  override def shortName(): String = "twitter"
}


object TwitterStreamingSource {

  val DEBUG_LEVEL = "debugLevel"
  val NUM_PARTITIONS = "numPartitions"
  val QUEUE_SIZE = "queueSize"
  val SCHEMA= ScalaReflection.schemaFor[MyTweet].dataType.asInstanceOf[StructType]

  /*  Current Schema - extracted by reflection
  val SCHEMA =
    StructType(
      StructField("id",StringType)::
        StructField("text", StringType) ::
        StructField("user_id", StringType) ::
        StructField("username", StringType) ::
        StructField("name", StringType) ::
        StructField("users", StringType)
        StructField("users", ArrayType) ::
        StructField("userLang", StringType) ::
        StructField("createdDate", StringType) ::
        StructField("in_reply_to_user_id", StringType) ::
        Nil)

   */
}


