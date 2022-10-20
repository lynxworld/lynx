package org.grapheco.lynx.types.time

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.time.LynxComponentTime.{getHourMinuteSecond, getNanosecond}
import org.grapheco.lynx.types.time.LynxComponentTimeZone.getZone
import org.grapheco.lynx.util.LynxTemporalParseException
import org.opencypher.v9_0.util.symbols.CTLocalTime

import java.time.LocalTime

/**
 * @ClassName LynxLocalTime
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxLocalTime(localTime: LocalTime) extends LynxTemporalValue with LynxComponentTime {
  def value: LocalTime = localTime

  def lynxType: LynxType = CTLocalTime

  override def sameTypeCompareTo(o: LynxValue): Int = ???

  //LynxComponentTime
  var hour: Int = localTime.getHour
  var minute: Int = localTime.getMinute
  var second: Int = localTime.getSecond
  var microsecond: Int = localTime.getNano * Math.pow(0.1, 6).toInt
  var millisecond: Int = (localTime.getNano * Math.pow(0.1, 3) % Math.pow(10, 3)).toInt
  var nanosecond: Int = localTime.getNano % Math.pow(10, 3).toInt
  var fraction: Int = localTime.getNano
}

object LynxLocalTime {

  def now(): LynxLocalTime = LynxLocalTime(LocalTime.now())

  def of(localTime: LocalTime): LynxLocalTime = LynxLocalTime(localTime)

  def of(hour: Int, minute: Int, second: Int, nanosOfSecond: Int): LynxLocalTime =
    LynxLocalTime(LocalTime.of(hour, minute, second, nanosOfSecond))

  def parse(localTimeStr: String): LynxLocalTime = LynxLocalTime(LocalTime.parse(localTimeStr))

  def parse(map: Map[String, Any]): LynxTemporalValue = {
    if (map.isEmpty) {
      throw LynxTemporalParseException("At least one temporal unit must be specified")
    }
    var v: LocalTime = null
    if (map.contains("timezone")) {
      if (map.size == 1) {
        of(LocalTime.now(getZone(map("timezone") match {
          case v: String => v
          case v: Any => v.toString
        })))
      } else {
        throw LynxTemporalParseException("Cannot assign time zone if also assigning other fields")
      }
    }
    else if (map.contains("hour")) {
      val (hour, minute, second) = getHourMinuteSecond(map, false)
      val nanoOfSecond = getNanosecond(map, true)
      of(hour, minute, second, nanoOfSecond)
    }
    else throw LynxTemporalParseException("parse date from map: map not contains (hour, minute, second) ")
  }
}
