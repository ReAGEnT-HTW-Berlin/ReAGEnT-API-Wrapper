package twitter

import utils.JSONUtils

import scala.io.BufferedSource

class TwitterConnectionMock extends TwitterConnection{

  var tt:twitterThread= _
  var bufSource: BufferedSource = _
  val TWEET_DELAY_IN_MS= 100
  class twitterThread extends Thread {

    override def run() {
      // Delay necessary to create
      Thread.sleep(2000)
      val bufReader= bufSource.bufferedReader
      while(running.get) {

        // readln blocks until the next line could be read
        val line = bufReader.readLine
        if (line==null) running.set(false)
        else {
          try {

            val t: Option[Any] = JSONUtils.parseJson(line)
            val jsonMap = t.get.asInstanceOf[Map[String, Any]]
            val tweet = jsonMap.get("tweet") match {
              case Some(s: String) => s
              case _ => ""
            }

              Thread.sleep(TWEET_DELAY_IN_MS)
              synchronized {sendTweetToListeningClasses(tweet)}
          } catch {
            case e: Exception => println("****kein Create MockTweet möglich****" + line + "***")
          }
        }
      }
    }
  }

  override def initialize:Unit={

    bufSource= scala.io.Source.fromFile("testData.txt")
    tt= new twitterThread
    tt.start
  }

  override def stop: Unit = {
    println("************called Twitter Stop*************")
    running.set(false)
  }
}

object TwitterConnectionMock extends TwitterConnection {

  var twiCon:TwitterConnection= new TwitterConnectionMock

  def createTwitterConnection:TwitterConnection={

    twiCon
  }

  override def stop={
    twiCon.stop
  }
}
