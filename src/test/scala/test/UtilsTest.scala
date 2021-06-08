package test

import org.scalatest.funsuite.AnyFunSuite
import utils.Utils
class UtilsTest extends AnyFunSuite{

  test("MdB getTest"){

    val res=Utils.getMdBs("BundestagTwitterAccounts.csv")
    res.foreach(println)
  }

  test("MdB getOserRules"){

    val mdb=Utils.getMdBs("BundestagTwitterAccounts.csv")
    val res=Utils.createUserRules(mdb,20)
    res.foreach(println)
    res.foreach(x=>println(x.length))
  }
}
