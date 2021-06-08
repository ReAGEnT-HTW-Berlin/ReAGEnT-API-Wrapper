package test

import org.scalatest.funsuite.AnyFunSuite
import twitter.MyTweet

class TweetCreateTest extends AnyFunSuite {

  val tweet= """{
               |  "data": {
               |    "in_reply_to_user_id": "36713389",
               |    "author_id": "838117368660393986",
               |    "entities": {
               |      "urls": [
               |        {
               |          "start": 59,
               |          "end": 82,
               |          "url": "https://t.co/6dDShNZPDp",
               |          "expanded_url": "https://youtu.be/Sqt3LZy0lyk",
               |          "display_url": "youtu.be/Sqt3LZy0lyk",
               |          "images": [
               |            {
               |              "url": "https://pbs.twimg.com/news_img/1388743848407572482/n81VNnb4?format=jpg&name=orig",
               |              "width": 1280,
               |              "height": 720
               |            },
               |            {
               |              "url": "https://pbs.twimg.com/news_img/1388743848407572482/n81VNnb4?format=jpg&name=150x150",
               |              "width": 150,
               |              "height": 150
               |            }
               |          ],
               |          "status": 200,
               |          "title": "Biologe analysiert: Wie sicher und wie wirksam ist der Corona-Impfstoff? (Clemens Arvay)",
               |          "description": "Corona-Impfstoff: Gefährlich und unwirksam? Bestseller-Autor und Biologe Clemens Arvay erklärt fachlich und verständlich, was viele Menschen bereits vermutet...",
               |          "unwound_url": "https://www.youtube.com/watch?v=Sqt3LZy0lyk&feature=youtu.be"
               |        }
               |      ],
               |      "mentions": [{ "start": 0, "end": 13, "username": "reitschuster" }]
               |    },
               |    "created_at": "2021-05-02T11:29:00.000Z",
               |    "id": "1388817798642782209",
               |    "text": "@reitschuster Corona-Impfstoff: \nGefährlich und unwirksam?\nhttps://t.co/6dDShNZPDp"
               |  },
               |  "includes": {
               |    "users": [
               |      {
               |        "id": "838117368660393986",
               |        "name": "SachsenKlartext",
               |        "username": "SachsenKlartext"
               |      },
               |      {
               |        "id": "36713389",
               |        "name": "Boris Reitschuster",
               |        "username": "reitschuster"
               |      }
               |    ]
               |  },
               |  "matching_rules": [
               |    { "id": 1371608279269445632, "tag": "Covid-19 Keywords Deutsch" }
               |  ]
               |}
               |""".stripMargin

  val tweet2= """{"data":{"author_id":"56129480","text":"mRNA Impfstoffe mit einer sehr hohen Wirksamkeit, \nendlich ein Malaria Impfstoff mit ebenfalls hoher Wirksamkeit (75%)…spannend was so alle im Bereich Wirkstoffe passiert (und was auch nicht)","created_at":"2021-05-04T09:50:08.000Z","id":"1389517692621701126"},"includes":{"users":[{"id":"56129480","name":"Beuteltier","username":"ein_beuteltier"}]},"matching_rules":[{"id":1371608279269445632,"tag":"Covid-19 Keywords Deutsch"}]}"""
  val tweet3= """{"data":{"author_id":"170964436","entities":{"mentions":[{"start":3,"end":15,"username":"BeschlossDC"}],"urls":[{"start":78,"end":101,"url":"https://t.co/RKeLHpNtEF","expanded_url":"https://twitter.com/BeschlossDC/status/1390993920000237568/photo/1","display_url":"pic.twitter.com/RKeLHpNtEF"}],"hashtags":[{"start":71,"end":77,"tag":"Getty"}],"annotations":[{"start":17,"end":25,"probability":0.8015,"type":"Person","normalized_text":"Churchill"},{"start":49,"end":54,"probability":0.9951,"type":"Place","normalized_text":"Europe"}]},"text":"RT @BeschlossDC: Churchill celebrates Victory in Europe, today 1945:   #Getty https://t.co/RKeLHpNtEF","created_at":"2021-05-08T19:36:20.000Z","id":"1391114765410443280"},"includes":{"users":[{"id":"170964436","name":"Linda Teuteberg","username":"LindaTeuteberg"}]},"matching_rules":[{"id":1390025979620470791,"tag":"MdBList3"}]}"""

  test("Create Tweet"){

    val t= MyTweet.createTweet(tweet)
    println(t)
  }

  test("Create Tweet2"){

    val t= MyTweet.createTweet(tweet2)
    println(t)
  }

  test("Create Tweet3"){

    val t= MyTweet.createTweet(tweet3)
    println(t)
    println(t.get.hashtags)

  }

  test("Create Tweet4"){

    val t= MyTweet.createTweet(tweet2)
    println(t)
    println(t.get.hashtags)

  }

    test("Create Tweet5"){

    val t= MyTweet.createTweet(tweet)
    println(t)
    //println(t.get.hashtags)

  }
}
