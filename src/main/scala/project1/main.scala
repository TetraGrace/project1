package project1

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession

object main {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("project1")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array(5,10,30))
    println(rdd.reduce(_+_))
  }
}
