package org.grapheco.lynx.procedure

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.{LynxInteger, LynxNumber, LynxString}
import org.grapheco.lynx.types.time._
import org.grapheco.lynx.util._

import java.time.{ZoneId, ZonedDateTime}

class TimeFunctions {
  @LynxProcedure(name = "lynx")
  def lynx(): String = {
    "lynx-0.3"
  }

  @LynxProcedure(name = "power")
  def power(x: LynxInteger, n: LynxInteger): Int = {
    math.pow(x.value, n.value).toInt
  }


  @LynxProcedure(name = "date")
  def date(inputs: LynxValue): LynxDate = {
    inputs match {
      case LynxString(v) => LynxDateUtil.parse(v)
      case LynxMap(v) => LynxDateUtil.parse(v).asInstanceOf[LynxDate]
    }
  }

  @LynxProcedure(name = "date")
  def date(): LynxDate = {
    LynxDateUtil.now()
  }

  @LynxProcedure(name = "datetime")
  def datetime(inputs: LynxValue): LynxDateTime = {
    LynxDateTimeUtil.parse(inputs).asInstanceOf[LynxDateTime]
  }

  @LynxProcedure(name = "datetime")
  def datetime(): LynxDateTime = {
    LynxDateTimeUtil.now()
  }

  @LynxProcedure(name = "localdatetime")
  def localDatetime(inputs: LynxValue): LynxLocalDateTime = {
    LynxLocalDateTimeUtil.parse(inputs).asInstanceOf[LynxLocalDateTime]
  }

  @LynxProcedure(name = "localdatetime")
  def localDatetime(): LynxLocalDateTime = {
    LynxLocalDateTimeUtil.now()
  }

  @LynxProcedure(name = "time")
  def time(inputs: LynxValue): LynxTime = {
    LynxTimeUtil.parse(inputs).asInstanceOf[LynxTime]
  }

  @LynxProcedure(name = "time")
  def time(): LynxTime = {
    LynxTimeUtil.now()
  }

  @LynxProcedure(name = "localtime")
  def localTime(inputs: LynxValue): LynxLocalTime = {
    LynxLocalTimeUtil.parse(inputs).asInstanceOf[LynxLocalTime]
  }

  @LynxProcedure(name = "localtime")
  def localTime(): LynxLocalTime = {
    LynxLocalTimeUtil.now()
  }

  @LynxProcedure(name = "duration")
  def duration(input: LynxValue): LynxDuration = {
    input match {
      case LynxString(v) => LynxDurationUtil.parse(v)
      case LynxMap(v) => LynxDurationUtil.parse(v.asInstanceOf[Map[String, LynxNumber]].mapValues(_.number.doubleValue()))
    }
  }


  @LynxProcedure(name = "date.statement")
  def statement(): LynxDate = {
    LynxDateUtil.now()
  }

  @LynxProcedure(name = "date.realtime")
  def date_realtime(inputs: LynxValue): LynxDate = {
    LynxDateUtil.parse(LynxMap(Map("timezone" -> inputs))).asInstanceOf[LynxDate]
  }

  @LynxProcedure(name = "date.realtime")
  def date_realtime(): LynxDate = {
    LynxDateUtil.now()
  }


  @LynxProcedure(name = "date.truncate")
  def truncate(unitStr: LynxString, dateValue: LynxDateTime, mapOfComponents: LynxMap): LynxDate = {
    LynxDateUtil.parse(Map("unitStr" -> unitStr, "dateValue" -> dateValue, "mapOfComponents" -> mapOfComponents))
  }

  @LynxProcedure(name = "date.truncate")
  def truncate(unitStr: LynxString, dateValue: LynxDateTime): LynxDate = {
    LynxDateUtil.parse(Map("unitStr" -> unitStr, "dateValue" -> dateValue))
  }

  @LynxProcedure(name = "date.truncate")
  def truncate(): LynxDate = {
    LynxDateUtil.now()
  }
}

