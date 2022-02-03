package project1

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import MenuStuff.Menu
import scala.io.StdIn.readLine
import Validation._

object main {
  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("Project1")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
   println("Spark session created")

    def p1(): Unit={
      println("hekllo")
    }
    def p2(): Unit={
      println("Under construction, please try again later")
    }
    def p3(): Unit={
      println("Under construction, please try again later")
    }
    def p4(): Unit={
      println("Under construction, please try again later")
    }
    def p5(): Unit={
      println("Under construction, please try again later")
    }
    def p6(): Unit={
      println("Under construction, please try again later")
    }
    //Let get our data in and try and do something with it


    val tempSelections = Map(1->"p1",2->"p2",3->"p3",4->"p4",5->"p5",6->"p6");
    val menu = new Menu(tempSelections)
    menu.printMenu()
    var choice = ""
    do {
      choice = readLine("Please choose an option")
    } while (!isInt(choice));

    val options = menu.selectOption(choice.toInt);
    options match {
      case "p1" =>p1()
    }
  }
}
