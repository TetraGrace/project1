package project1

import scala.util.Try

object Validation {
  def isInt(in:String):Boolean={
    //checks if a given string is an int, and returns false if not
    if(Try(in.toInt).isSuccess){
      return true
    }
    return false
  }
}
