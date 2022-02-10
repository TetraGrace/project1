package project1

import scala.collection.immutable.ListMap

object MenuStuff {
  class Menu(options:Map[Int,String]) {
    //map of all the map options. The int is in the position of the menu option starting at 0. The string is the return value for when its selected
    var menuOptions = ListMap(options.toSeq.sortBy(_._1):_*);

    private def printMenuLine(): Unit = {
      //prints a line that goes on the top of bottom of a menu
      println("+------------------------------------------------+")
    }

    private def printMenuOption(ind: Int, menVal: String): Unit = {
      //Prints out a section of the menu that is a set amount of characters long.
      val maxLength = 50;
      var tempStr = "| "
      tempStr += ind.toString + " -> " + menVal
      while (tempStr.length < (maxLength - 1)) {
        tempStr += " "
      }
      tempStr += "|"
      println(tempStr)
    }

    private def printEmptyMenuLine(): Unit = {
      //prints and empty menu line to avoid confusion
      println("|                                                |")
    }

    def printMenu(): Unit = {
      //prints the entire menu with all options
      println("+-Options----------------------------------------+")
      menuOptions.foreach(m => ({printMenuOption(m._1,m._2)}))
      printMenuLine()
    }
    def selectOption(cho: Int):String = {
      return menuOptions(cho);
    }
    def addMenuOption(item:(Int, String)):Unit={
      menuOptions+=item
    }
  }
}
