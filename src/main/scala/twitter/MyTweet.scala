package twitter

import java.util

import org.apache.spark.sql.Row
import org.bson.Document
import utils.TweetWriteMongoConnection

case class MyTweet(
                    id: String,
                    text: String,
                    user_id: String,
                    username: String,
                    name: String,
                    hashtags: List[String],
                    users: List[User],
                    party: String,
                    createdDate: String,
                    in_reply_to_user_id: String,
                    source: String,
                    json: String
                    //                    processingTime: TimeStamp
                  ) {

  def toDocument: Document = {

    val m: java.util.Map[String, Object] = new java.util.HashMap()
    m.put("id", id)
    m.put("text", text)
    m.put("user_id", user_id)
    m.put("username", username)
    m.put("name", name)
    val tags = new util.ArrayList[String]
    hashtags.foreach(ht => tags.add(ht))
    m.put("hashtags", tags)
    val ulist = new util.ArrayList[java.util.Map[String, Object]]
    users.foreach(u => {
      val q: java.util.Map[String, Object] = new java.util.HashMap()
      q.put("id", u.user_id)
      q.put("username", u.username)
      q.put("name", u.name)
      ulist.add(q)
    })
    m.put("users", ulist)
    m.put("party", party)
    m.put("createdDate", createdDate)
    m.put("in_reply_to_user_id", in_reply_to_user_id)
    m.put("source", source)
    new Document(m)
  }

}

case class User(user_id: String, username: String, name: String)

case object MyTweet {

  val mongoCon = new TweetWriteMongoConnection("examples", "seija", "RhoMTXB2", "bson", "json")
  //  final val mdbs: Map[String, MdB] = Utils.getMdBs("BundestagTwitterAccounts.csv")

  def createTweet(json: String): Option[MyTweet] = {

    try {
      val t: Option[Any] = JSONUtils.parseJson(json)


      val jsonMap = t.get.asInstanceOf[Map[String, Any]]
      val data: Option[Any] = jsonMap.get("data")
      val user = jsonMap.get("includes").get.asInstanceOf[Map[String, Any]].
        get("users")
      val entities = data.get.asInstanceOf[Map[String, Any]].getOrElse("entities", Map().asInstanceOf[Map[String, Any]])
      val party = jsonMap.get("matching_rules").get.asInstanceOf[List[Map[String, Any]]].flatten.toMap.getOrElse("tag", "")

      // Extract tweet data to parse in our own format
      (data, user, entities, party) match {
        case (Some(d: Map[String, Any]), Some(u: List[Any]), e: Map[String, Any], f: String) => {

          (
            d.get("id"),
            d.get("text"),
            d.get("created_at"),
            d.get("author_id": String),
            d.get("source": String),
            f,
            extractUsers(d.getOrElse("author_id", "").asInstanceOf[String], u.asInstanceOf[List[Map[String, Any]]]),
            extractHashtags(e.getOrElse("hashtags", List()).asInstanceOf[List[Map[String, Any]]])) match {

            case (
              Some(id: String),
              Some(text: String),
              Some(created_at: String),
              Some(author_id: String),
              Some(source: String),
              f,
              (author_name: String, name: String, userList: List[User]),
              tagList: List[String]) => {

              val party = f.asInstanceOf[String]
              val inReply = d.getOrElse("in_reply_to_user_id", "").asInstanceOf[String]
              Some(MyTweet(id, text, author_id, author_name, name, tagList, userList, party, created_at, inReply, source, json))
            }
            case _ => None
          }
        }
        case _ => None
      }

    }
    catch {
      case e: Exception => handleErrors(json);None //println("****waiting****" + json + "***"); None
    }
  }


  def handleErrors(json: String): Unit = {
    try {
      val t: Option[Any] = JSONUtils.parseJson(json)
      val jsonMap = t.get.asInstanceOf[Map[String, Any]]
      val errors: Option[Any] = jsonMap.get("errors")

      println("restarting . . .")
      TwitterConnectionImpl.stop
      ReAGEnT_API_Wrapper.stop()
    }
    catch {
      case e: Exception => None
    }
    None
  }

  def extractUsers(author_id: String, userList: List[Map[String, Any]]): (String, String, List[User]) = {
    val users = for (u <- userList) yield {
      User(u("id").asInstanceOf[String], u("username").asInstanceOf[String], u("name").asInstanceOf[String])
    }
    val user = users.filter(u => author_id.equals(u.user_id)).head
    (user.username, user.name, users.toList)
  }

  def extractHashtags(tagList: List[Map[String, Any]]): List[String] = {

    tagList.flatMap(el => {
      val t = el.getOrElse("tag", "");
      if (t == "") List() else List(t)
    }).toList.asInstanceOf[List[String]]
  }

  def tweetToRow(myTweet: MyTweet): Row = {
    Row(myTweet.id, myTweet.text, myTweet.user_id, myTweet.username, myTweet.name, myTweet.hashtags,
      myTweet.users, myTweet.party, myTweet.createdDate, myTweet.in_reply_to_user_id, myTweet.source, myTweet.json)
  }

  def createTweetFromRow(row: Row): MyTweet = {

    MyTweet(row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4),
      row.get(5).asInstanceOf[scala.collection.mutable.WrappedArray[String]].toList, {

        val u_row = row.get(6).asInstanceOf[scala.collection.mutable.WrappedArray[Row]]
        val users = u_row.map(u => User(u.getString(0), u.getString(1), u.getString(2)))
        users.toList
      }, row.getString(7), row.getString(8), row.getString(9), row.getString(10), row.toString())
  }

}

/*
{"data":{"author_id":"1238140547489030146","id":"1388441831680188419","text":"@BetzeSGT @db_qhd @ZDFheute Kann nicht and genau sagen. Es sind aber sich schon einige Kinder an Corona gestorben. In den Rohdaten des RKI mehr als in der Übersicht. Da angeblich nur mit und nicht an.\nKann man dann vergleichen, ob mit oder ohne Impfung schlimmer war. Wird ja sicher beides geben. 😔","entities":{"mentions":[{"start":0,"end":9,"username":"BetzeSGT"},{"start":10,"end":17,"username":"db_qhd"},{"start":18,"end":27,"username":"ZDFheute"}]},"created_at":"2021-05-01T10:35:03.000Z"},"includes":{"users":[{"id":"1238140547489030146","name":"MEISU🔴🔴🔴","username":"MEISU98037829"}]},"matching_rules":[{"id":1371608279269445632,"tag":"Covid-19 Keywords Deutsch"}]}

 */