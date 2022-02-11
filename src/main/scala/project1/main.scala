package project1

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import MenuStuff.Menu
import scala.io.StdIn.readLine
import Validation._

object main {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Hadoop")

    val spark = SparkSession.builder()
      .appName("Project1")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")


    println("Spark session created")
    /*
    println("loading Data from data Files.")
    spark.sql("DROP TABLE BEVBA;")
    spark.sql("DROP TABLE BEVBB;")
    spark.sql("DROP TABLE BEVBC;")
    spark.sql("DROP TABLE BEVCONSA;")
    spark.sql("DROP TABLE BEVCONSB;")
    spark.sql("DROP TABLE BEVCONSc;")
    spark.sql("create table BevBA(DRINK string, branch string)row format delimited fields terminated by ',';")
    spark.sql("LOAD DATA INPATH 'I:/try4/project1/Data/Bev_BranchA.txt' into table BevBA;")
    spark.sql("create table BevBB(DRINK string, branch string)row format delimited fields terminated by ',';")
    spark.sql("LOAD DATA INPATH 'I:/try4/project1/Data/Bev_BranchB.txt' into table BevBB;")
    spark.sql("create table BevBC(DRINK string, branch string)row format delimited fields terminated by ',';")
    spark.sql("LOAD DATA INPATH 'I:/try4/project1/Data/Bev_BranchC.txt' into table BevBC;")
    spark.sql("create table BevConsA(DRINK string, amount string)row format delimited fields terminated by ',';")
    spark.sql("LOAD DATA INPATH 'I:/try4/project1/Data/Bev_ConscountA.txt' into table BevConsA;")
    spark.sql("create table BevConsB(DRINK string, amount string)row format delimited fields terminated by ',';")
    spark.sql("LOAD DATA INPATH 'I:/try4/project1/Data/Bev_ConscountB.txt' into table BevConsB;")
    spark.sql("create table BevConsC(DRINK string, amount string)row format delimited fields terminated by ',';")
    spark.sql("LOAD DATA INPATH 'I:/try4/project1/Data/Bev_ConscountC.txt' into table BevConsc;")
    spark.sql("create table p1Branches(DRINK string, branch string)row format delimited fields terminated by ',';")
    spark.sql("LOAD DATA INPATH 'I:/try4/project1/Data/Bev_BranchA.txt' into table p1Branches;")
    spark.sql("LOAD DATA INPATH 'I:/try4/project1/Data/Bev_BranchB.txt' into table p1Branches;")
    spark.sql("LOAD DATA INPATH 'I:/try4/project1/Data/Bev_BranchC.txt' into table p1Branches;")
    spark.sql("create table p1Consume(DRINK string, amount string)row format delimited fields terminated by ',';")
    spark.sql("LOAD DATA INPATH 'I:/try4/project1/Data/Bev_ConscountA.txt' into table p1Consume;")
    spark.sql("LOAD DATA INPATH 'I:/try4/project1/Data/Bev_ConscountB.txt' into table p1Consume;")
    spark.sql("LOAD DATA INPATH 'I:/try4/project1/Data/Bev_Conscountc.txt' into table p1Consume;")
    spark.sql("SELECT * FROM p1Branches;").show()
    spark.sql("SELECT * FROM p1Consume;").show()
    spark.sql("SELECT * FROM BevBA;").show();
    spark.sql("SELECT * FROM BevBB;").show();
    spark.sql("SELECT * FROM BevBC;").show();
    spark.sql("SELECT * FROM BevConsA;").show();
    spark.sql("SELECT * FROM BevConsB;").show();
    spark.sql("SELECT * FROM BevConsC;").show();
*/

    def p1(): Unit = {
      println("Problem: 1.")
      println("What is th total number of consumers for Branch1?")
      spark.sql("SELECT SUM(c.amount) FROM p1Consume c JOIN p1Branches b on (b.drink = c. drink) and (b.branch = 'Branch1')").show()
      println("What is the total Number of consumers for Branch2?")
      val b1 = spark.sql("SELECT SUM(c.amount) FROM p1Consume c JOIN p1Branches b on (b.drink = c. drink) and (b.branch = 'Branch2')").show()
    }

    def p2(): Unit = {
      println("What is Branch1s most consumed beverage?")
      spark.sql("SELECT c.drink FROM p1Consume c JOIN p1Branches b on (b.branch = 'Branch1') and(b.drink = c.drink) group by c.drink order by SUM(c.amount) DESC;").show(1)
      println("What is Branch2s least consumed beverage?")
      spark.sql("SELECT c.drink FROM p1Consume c JOIN p1Branches b on (b.branch = 'Branch2') and(b.drink = c.drink) group by c.drink order by SUM(c.amount) ASC;").show(1)
      println("What is branch 2 average beverage?")
      spark.sql("SELECT c.drink FROM p1Consume c JOIN p1Branches b on (b.branch = 'Branch2') and(b.drink = c.drink) group by c.drink order by AVG(c.amount) DESC;").show(1)

    }
    def p3(): Unit = {
      println("Drinks avalible form branch8")
      spark.sql("SELECT drink FROM p1Branches where branch = 'Branch8';").show(100)
      println("Drinks avalible form branch1")
      spark.sql("SELECT drink FROM p1Branches where branch = 'Branch1';").show(100)
      println("Drinks avalible form branch10")
      spark.sql("SELECT drink FROM p1Branches where branch = 'Branch10';").show(100)
      println("The drinks in common between Branch4 and Branch7.")
      spark.sql("CREATE TABLE IF NOT EXISTS p1Copy SELECT * FROM p1Branches;")
      spark.sql("SELECT a.drink from p1Branches a where a.branch = 'Branch4' INTERSECT SELECT b.drink FROM p1Branches b WHERE b.branch = 'Branch7';").show(100)
    }

    def p4(): Unit = {
      spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
      println("Making a partition.")
      spark.sql("create table IF NOT EXISTS p4(branch String, drink String) partitioned by (branch)")
      spark.sql("DESCRIBE p4;").show();
      println("Deleting The first instance of branch1 from p1copy.")
      spark.sql("Drop table if exists pfour;")
      spark.sql("create table IF NOT EXISTS pfour select * from p1Copy where not branch = 'Branch6';")
      println("Original")
      spark.sql("Select * from p1copy").show()
      println("Without the first instance")
      spark.sql("Select * from pfour").show()
    }

    def p5(): Unit = {
        spark.sql("alter table p1copy set tblproperties('notes' = 'This is a note');");
      spark.sql("DESCRIBE EXTENDED p1copy;").show(1000)
      spark.sql("alter table p1copy set tblproperties('comment' = 'This is a comment');")
      spark.sql("DESCRIBE EXTENDED p1copy;").show(1000)
    }

    def p6(): Unit = {
      println("Under construction, please try again later")
      //alright lets get funky
      val res1a = spark.sql("SELECT sum(a.amount) FROM bevconsa a JOIN bevba b on b.drink = a.drink where b.branch = 'Branch1';").first().toString()
      val res2a = spark.sql("SELECT sum(a.amount) FROM bevconsa a JOIN bevba b on b.drink = a.drink where b.branch = 'Branch2';").first().toString()
      val res3a = spark.sql("SELECT sum(a.amount) FROM bevconsa a JOIN bevba b on b.drink = a.drink where b.branch = 'Branch3';").first().toString()
      val res4a = spark.sql("SELECT sum(a.amount) FROM bevconsa a JOIN bevba b on b.drink = a.drink where b.branch = 'Branch4';").first().toString()
      val res5a = spark.sql("SELECT sum(a.amount) FROM bevconsa a JOIN bevba b on b.drink = a.drink where b.branch = 'Branch5';").first().toString()
      val res6a = spark.sql("SELECT sum(a.amount) FROM bevconsa a JOIN bevba b on b.drink = a.drink where b.branch = 'Branch6';").first().toString()
      val res7a = spark.sql("SELECT sum(a.amount) FROM bevconsa a JOIN bevba b on b.drink = a.drink where b.branch = 'Branch7';").first().toString()
      val res8a = spark.sql("SELECT sum(a.amount) FROM bevconsa a JOIN bevba b on b.drink = a.drink where b.branch = 'Branch8';").first().toString()
      val res9a = spark.sql("SELECT sum(a.amount) FROM bevconsa a JOIN bevba b on b.drink = a.drink where b.branch = 'Branch9';").first().toString()

      val res1b = spark.sql("SELECT sum(a.amount) FROM bevconsb a JOIN bevbb b on b.drink = a.drink where b.branch = 'Branch1';").first().toString()
      val res2b = spark.sql("SELECT sum(a.amount) FROM bevconsb a JOIN bevbb b on b.drink = a.drink where b.branch = 'Branch2';").first().toString()
      val res3b = spark.sql("SELECT sum(a.amount) FROM bevconsb a JOIN bevbb b on b.drink = a.drink where b.branch = 'Branch3';").first().toString()
      val res4b = spark.sql("SELECT sum(a.amount) FROM bevconsb a JOIN bevbb b on b.drink = a.drink where b.branch = 'Branch4';").first().toString()
      val res5b = spark.sql("SELECT sum(a.amount) FROM bevconsb a JOIN bevbb b on b.drink = a.drink where b.branch = 'Branch5';").first().toString()
      val res6b = spark.sql("SELECT sum(a.amount) FROM bevconsb a JOIN bevbb b on b.drink = a.drink where b.branch = 'Branch6';").first().toString()
      val res7b = spark.sql("SELECT sum(a.amount) FROM bevconsb a JOIN bevbb b on b.drink = a.drink where b.branch = 'Branch7';").first().toString()
      val res8b = spark.sql("SELECT sum(a.amount) FROM bevconsb a JOIN bevbb b on b.drink = a.drink where b.branch = 'Branch8';").first().toString()
      val res9b = spark.sql("SELECT sum(a.amount) FROM bevconsb a JOIN bevbb b on b.drink = a.drink where b.branch = 'Branch9';").first().toString()

      val res1c = spark.sql("SELECT sum(a.amount) FROM bevconsc a JOIN bevbc b on b.drink = a.drink where b.branch = 'Branch1';").first().toString()
      val res2c = spark.sql("SELECT sum(a.amount) FROM bevconsc a JOIN bevbc b on b.drink = a.drink where b.branch = 'Branch2';").first().toString()
      val res3c = spark.sql("SELECT sum(a.amount) FROM bevconsc a JOIN bevbc b on b.drink = a.drink where b.branch = 'Branch3';").first().toString()
      val res4c = spark.sql("SELECT sum(a.amount) FROM bevconsc a JOIN bevbc b on b.drink = a.drink where b.branch = 'Branch4';").first().toString()
      val res5c = spark.sql("SELECT sum(a.amount) FROM bevconsc a JOIN bevbc b on b.drink = a.drink where b.branch = 'Branch5';").first().toString()
      val res6c = spark.sql("SELECT sum(a.amount) FROM bevconsc a JOIN bevbc b on b.drink = a.drink where b.branch = 'Branch6';").first().toString()
      val res7c = spark.sql("SELECT sum(a.amount) FROM bevconsc a JOIN bevbc b on b.drink = a.drink where b.branch = 'Branch7';").first().toString()
      val res8c = spark.sql("SELECT sum(a.amount) FROM bevconsc a JOIN bevbc b on b.drink = a.drink where b.branch = 'Branch8';").first().toString()
      val res9c = spark.sql("SELECT sum(a.amount) FROM bevconsc a JOIN bevbc b on b.drink = a.drink where b.branch = 'Branch9';").first().toString()

      println("Branch1 y1 consumption: " + res1a)
      println("Branch2 y1 consumption: " + res2a)
      println("Branch3 y1 consumption: " + res3a)
      println("Branch4 y1 consumption: " + res4a)
      println("Branch5 y1 consumption: " + res5a)
      println("Branch6 y1 consumption: " + res6a)
      println("Branch7 y1 consumption: " + res7a)
      println("Branch8 y1 consumption: " + res8a)
      println("Branch9 y1 consumption: " + res9a)
      println()
      println("Branch1 y2 consumption: " + res1b)
      println("Branch2 y2 consumption: " + res2b)
      println("Branch3 y2 consumption: " + res3b)
      println("Branch4 y2 consumption: " + res4b)
      println("Branch5 y2 consumption: " + res5b)
      println("Branch6 y2 consumption: " + res6b)
      println("Branch7 y2 consumption: " + res7b)
      println("Branch8 y2 consumption: " + res8b)
      println("Branch9 y2 consumption: " + res9b)
      println()
      println("Branch1 y3 consumption: " + res1c)
      println("Branch2 y3 consumption: " + res2c)
      println("Branch3 y3 consumption: " + res3c)
      println("Branch4 y3 consumption: " + res4c)
      println("Branch5 y3 consumption: " + res5c)
      println("Branch6 y3 consumption: " + res6c)
      println("Branch7 y3 consumption: " + res7c)
      println("Branch8 y3 consumption: " + res8c)
      println("Branch9 y3 consumption: " + res9c)
    }


    val tempSelections = Map(1 -> "p1", 2 -> "p2", 3 -> "p3", 4 -> "p4", 5 -> "p5", 6 -> "p6", 0->"quit");
    val menu = new Menu(tempSelections)

    var choice = ""
    while (choice != "0") {
      menu.printMenu()
      choice = readLine("Please choose an option")
      if(isInt(choice)){
        val options = menu.selectOption(choice.toInt);
        options match {
          case "p1" => p1()
          case "p2" => p2()
          case "p3" => p3()
          case "p4" => p4()
          case "p5" => p5()
          case "p6" => p6()
          case "quit" => println("Quiting")
        }
      } else {
        println("Please enter a number.")
      }
    }
  }
}