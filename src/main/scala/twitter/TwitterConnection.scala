package twitter

import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.atomic.AtomicBoolean

abstract class TweetListener {
  def onTweet(tweet: String): Unit
}

trait TwitterConnection{

  var observerList:ArrayBuffer[TweetListener] = ArrayBuffer()
  var running = new AtomicBoolean(true)
  initialize

  def initialize:Unit={}
  def stop: Unit= {}

  def registerEventListener(listen:TweetListener):Unit= synchronized {

    observerList += listen
  }

  def removeEventListener(listen:TweetListener):Unit= synchronized {

    val index: Int = observerList.indexOf(listen)
    observerList.remove(index)
    println(observerList)
  }

  def sendTweetToListeningClasses(tweet:String): Unit= synchronized{

    observerList.foreach(t=> t.onTweet(tweet))
  }
}

