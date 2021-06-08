package twitter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}

import scala.collection.mutable.ListBuffer
class TweetStreamBatchTask(tweetList:ListBuffer[MyTweet]) extends InputPartition[InternalRow] {

override def createPartitionReader(): InputPartitionReader[InternalRow] = new TweetStreamBatchReader(tweetList)
}

/*
 TweetStreamBatchReader liest Daten eines Batches
 hat intern einen Zeiger currentIdx
 Implementiert Iterator
 */

class TweetStreamBatchReader(tweetList:ListBuffer[MyTweet]) extends InputPartitionReader[InternalRow] {

  private var currentIdx = -1
  import org.apache.spark.sql.Encoders
  val tweetEncoder = Encoders.product[MyTweet]
  import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
  val tweetExprEncoder = tweetEncoder.asInstanceOf[ExpressionEncoder[MyTweet]]

  override def next(): Boolean = {
    // Return true as long as the new index is in the seq.
    currentIdx += 1
  //  println("%%%%%%%%%% "+currentIdx+ " < "+tweetList.size+"%%%%%%%%%%%%%%%%%%")
    currentIdx < tweetList.size
  }

  override def get(): InternalRow = {

    val tweet = tweetList(currentIdx)
    tweetExprEncoder.toRow(tweet)

    /*

    /*

     DataType conversion - since Spark 2.4 necessary (API changes Row -> InternalRow )
    */
    def userToInternalRow(u:User):InternalRow={
      val id= UTF8String.fromString(u.user_id)
      val user= UTF8String.fromString(u.username)
      val name= UTF8String.fromString(u.name)
      InternalRow(id,user,name)
    }

    val id=UTF8String.fromString(tweet.id)
    val text=UTF8String.fromString(tweet.text)
    val user_id= UTF8String.fromString(tweet.user_id)
    val username= UTF8String.fromString(tweet.username)
    val name= UTF8String.fromString(tweet.name)
    val lang= UTF8String.fromString(tweet.userLang)
    val ctime= UTF8String.fromString(tweet.createdDate)
    val users= ArrayData.toArrayData(tweet.users.map(userToInternalRow))
    val in_reply_to_user_id= UTF8String.fromString(tweet.in_reply_to_user_id)

    //println(tweet.getCreatedAt.getTime+" ------> "+new Timestamp(tweet.getCreatedAt.getTime).toString)
    /*
      getTime wandelt ein Date in ein Long um, dass die ms nach dem 01.1.1970 abbildet
      Repräsentation entspricht nicht dem Timestamp von java.sql.Timestamp
      wurde mit Version 2.4 geändert
      Umwandlung in String möglich aber unschön - muss noch verbessert werden
     */
    val i=InternalRow(id,text,user_id,username, name, users, lang,ctime,in_reply_to_user_id)
    //println(i)
    i
     */
  }

  def close(): Unit = {}
}

