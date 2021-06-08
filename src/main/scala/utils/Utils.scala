package utils

import scala.io.Source

case class MdB(user_id:String, screen_name:String, name:String, party: String,
               location: String, created_at: String)

object Utils {

  def getMdBs(filename: String):Map[String,MdB]={

    val src: Iterator[String] = Source.fromFile(filename).getLines
    val srcWithoutHeader: Iterator[String] = src.drop(1)

    // processing remaining lines
    val members: Iterator[(String, MdB)] = { for (l <- srcWithoutHeader) yield {
      // split line by comma and process them
      val c1: Array[String] = l.split(";")
      val c=  c1.map(s=>s.trim)
      (c(1), MdB(c(1),c(0),c(2),c(3),c(4),c(5)))}}

    val m:Map[String,MdB]=members.toMap
    m
  }

  def createUserRules(map:Map[String, MdB], nr:Int):List[String]={

    var mList:List[String]= List()
    val mapIt= map.iterator
    var n=1
    while (mapIt.hasNext){
      val users= mapIt.take(nr)
      val names= users.map(n=> "from:"+n._2.screen_name).mkString(" OR ")

      val rule=  """{ "add": [ { "value": """"+ names + """ lang:de -is:retweet","""+
        """"tag": "MdBList"""+n+""""}]}"""
      n=n+1
      mList= rule::mList
    }
    mList
  }

}
