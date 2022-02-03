package project1

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import MenuStuff.Menu

object main {
  def main(args:Array[String]):Unit = {
//    val conf = new SparkConf().setMaster("local[*]").setAppName("project1")
//    val sc = new SparkContext(conf)
//
//    val rdd = sc.parallelize(Array(5,10,30))
//    println(rdd.reduce(_+_))

    val tempSelections = Map((0->"p1"),(1->"p2"),(2->"p2"),(1->"p2"),(1->"p2"),(1->"p2"),(1->"p2"));
    val menu = new Menu(tempSelections)
    menu.printMenu()
  }
}
