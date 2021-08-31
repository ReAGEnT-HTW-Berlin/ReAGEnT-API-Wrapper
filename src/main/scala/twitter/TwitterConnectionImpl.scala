package twitter

import scalaj.http._

import scala.io.BufferedSource

class TwitterConnectionImpl extends TwitterConnection {

  var tt: twitterThread = null

  class twitterThread extends Thread {

    override def run() {

      val TWITTER_FILTERED_STREAM_ENDPOINT = "https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at,entities,public_metrics,context_annotations,geo,referenced_tweets,reply_settings,source,withheld,attachments,possibly_sensitive&expansions=author_id,in_reply_to_user_id&user.fields=description&media.fields=media_key,type,duration_ms,public_metrics"
      val bearerToken = sys.env.getOrElse("TWITTER_BEARER", "")
      while (running.get) {
        println("receiving tweets...")

        val request: HttpRequest =
          Http(TWITTER_FILTERED_STREAM_ENDPOINT)
            .header("Authorization", s"Bearer ${bearerToken}")
            .timeout(Integer.MAX_VALUE, Integer.MAX_VALUE)
        try {
          request.execute(is => {
            if (is != null) {
              val bufSource: BufferedSource = scala.io.Source.fromInputStream(is)
              val bufReader = bufSource.bufferedReader
              while (running.get) {

                // readln blocks until the next line could be read
                val line = bufReader.readLine

                println(line)
                sendTweetToListeningClasses(line)
              }
            }
            else {
              println(".... Restart Connection Necessary .....")
            }
          })
        } catch {
          case e: Exception => println("Connection lost . . . leaving wrapper"); ReAGEnT_API_Wrapper.stop()
        }
      }
    }
  }

  override def initialize = {

    tt = new twitterThread
    tt.start
  }

  override def stop = {
    println("************called Twitter Stop*************")
    running.set(false)
  }
}

object TwitterConnectionImpl extends TwitterConnection {

  var twiCon: TwitterConnection = new TwitterConnectionImpl

  def createTwitterConnection: TwitterConnection = {

    twiCon
  }

  override def stop = {
    twiCon.stop
  }
}
