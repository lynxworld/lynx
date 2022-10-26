package org.grapheco.lynx.procedure

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}

/**
 * @ClassName StringFunctions
 * @Description These functions are used to manipulate strings or to create a string representation of another value
 * @Author Hu Chuan
 * @Date 2022/4/20
 * @Version 0.1
 */
class StringFunctions {
  @LynxProcedure(name = "left")
  def left(x: LynxString, endIndex: LynxInteger): String = {
    val str = x.value
    if (endIndex.value.toInt < str.length) str.substring(0, endIndex.value.toInt)
    else str
  }

  @LynxProcedure(name = "lTrim")
  def lTrim(x: LynxString): String = {
    val str = x.value
    if (str == "" || str == null) str
    else x.value.replaceAll(s"^[  ]+", "")
  }

  @LynxProcedure(name = "replace")
  def replace(x: LynxString, search: LynxString, replace: LynxString): String = {
    val str = x.value
    if (str == "" || str == null) str
    else str.replaceAll(search.value, replace.value)
  }


  /**
   * the method is moved to List function
   * @param x
   * @return
   */
//  @LynxProcedure(name = "reverse")
//  def reverse(x: LynxString): String = {
//    val str = x.value
//    if (str == "" || str == null) str
//    else str.reverse
//  }

  @LynxProcedure(name = "right")
  def right(x: LynxString, endIndex: LynxInteger): String = {
    val str = x.value
    if (endIndex.value.toInt < str.length) str.substring(endIndex.value.toInt - 1)
    else str
  }

  @LynxProcedure(name = "rTrim")
  def rTrim(x: LynxString): String = {
    val str = x.value
    if (str == "" || str == null) str
    else x.value.replaceAll(s"[　 ]+$$", "")
  }

  @LynxProcedure(name = "split")
  def split(x: LynxString, regex: LynxString): Array[String] = {
    val str = x.value
    if (str == "" || str == null) Array(str)
    else str.split(regex.value)
  }

  @LynxProcedure(name = "substring")
  def substring(x: LynxString, left: LynxInteger, length: LynxInteger): String = {
    val str = x.value
    if (str == "" || str == null) str
    else {
      if (left.value.toInt + length.value.toInt < str.length)
        str.substring(left.value.toInt, left.value.toInt + length.value.toInt)
      else str.substring(left.value.toInt)
    }
  }

  @LynxProcedure(name = "substring")
  def substring(x: LynxString, left: LynxInteger): String = {
    val str = x.value
    if (str == "" || str == null) str
    else str.substring(left.value.toInt)
  }

  @LynxProcedure(name = "toLower")
  def toLower(x: LynxString): String = {
    x.value.toLowerCase
  }

  @LynxProcedure(name = "toString")
  def toString(x: LynxValue): String = x match {
    //    case dr: LynxDuration => dr.toString
    case _ => x.value.toString
  }

  @LynxProcedure(name = "toUpper")
  def toUpper(x: LynxString): String = {
    x.value.toUpperCase
  }

  @LynxProcedure(name = "trim")
  def trim(x: LynxString): String = {
    val str = x.value
    if (str == "" || str == null) str
    else str.trim
  }
}
