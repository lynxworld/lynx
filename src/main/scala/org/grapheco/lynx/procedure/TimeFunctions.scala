package org.grapheco.lynx.procedure

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.{LynxInteger, LynxNumber, LynxString}
import org.grapheco.lynx.types.time._
import org.grapheco.lynx.util._

import java.time.{ZoneId, ZonedDateTime}
import java.util.Date

class TimeFunctions {
  @LynxProcedure(name = "lynx")
  def lynx(): String = {
    "lynx-0.3"
  }


//  @LynxProcedure(name = "timestamp")
//  def timestamp(): LynxValue = {
//   LynxValue(new Date().getTime())
//  }
  @LynxProcedure(name = "power")
  def power(x: LynxInteger, n: LynxInteger): Int = {
    math.pow(x.value, n.value).toInt
  }


  @LynxProcedure(name = "date")
  def date(inputs: LynxValue): LynxDate = {
    inputs match {
      case LynxString(v) => LynxDate.parse(v)
      case LynxMap(v) => LynxDate.parse(v)
    }
  }

  @LynxProcedure(name = "date")
  def date(): LynxDate = {
    LynxDate.now()
  }

  @LynxProcedure(name = "datetime")
  def datetime(inputs: LynxValue): LynxDateTime = {
    inputs match {
      case LynxString(v) => LynxDateTime.parse(v)
      case LynxMap(v) => LynxDateTime.parse(inputs).asInstanceOf[LynxDateTime]
    }

  }


  @LynxProcedure(name = "datetime")
  def datetime(): LynxDateTime = {
    LynxDateTime.now()
  }

  @LynxProcedure(name = "localdatetime")
  def localDatetime(inputs: LynxValue): LynxLocalDateTime = {
    LynxLocalDateTime.parse(inputs).asInstanceOf[LynxLocalDateTime]
  }

  @LynxProcedure(name = "localdatetime")
  def localDatetime(): LynxLocalDateTime = {
    LynxLocalDateTime.now()
  }

  @LynxProcedure(name = "time")
  def time(inputs: LynxValue): LynxTime = {
    inputs match {
      case LynxString(v) => LynxTime.parse(v)
      case LynxMap(v) => LynxTime.parse(v)
    }
  }

  @LynxProcedure(name = "time")
  def time(): LynxTime = {
    LynxTime.now()
  }

  @LynxProcedure(name = "localtime")
  def localTime(inputs: LynxValue): LynxLocalTime = {
    inputs match {
      case LynxString(v) => LynxLocalTime.parse(v)
      case LynxMap(v) => LynxLocalTime.parse(v).asInstanceOf[LynxLocalTime]
    }
  }

  @LynxProcedure(name = "localtime")
  def localTime(): LynxLocalTime = {
    LynxLocalTime.now()
  }

  @LynxProcedure(name = "localtime.truncate")
  def localtimeTruncate(unitStr: LynxString, dateValue: LynxTime, mapOfComponents: LynxMap): LynxLocalTime = {
    LynxLocalTime.parse(Map("unitStr" -> unitStr, "timeValue" -> dateValue, "mapOfComponents" -> mapOfComponents))
  }

  @LynxProcedure(name = "localtime.truncate")
  def localtimeTruncate(unitStr: LynxString, dateValue: LynxTime): LynxLocalTime = {
      LynxLocalTime.parse(Map("unitStr" -> unitStr, "timeValue" -> dateValue))
  }

  @LynxProcedure(name = "localtime.truncate")
  def localtimeTruncate(): LynxLocalTime = {
    LynxLocalTime.now()
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
    LynxDate.now()
  }

  @LynxProcedure(name = "date.realtime")
  def date_realtime(inputs: LynxValue): LynxDate = {
    LynxDate.parse(Map("timezone" -> inputs)).asInstanceOf[LynxDate]
  }

  @LynxProcedure(name = "date.realtime")
  def date_realtime(): LynxDate = {
    LynxDate.now()
  }


  @LynxProcedure(name = "date.truncate")
  def truncate(unitStr: LynxString, dateValue: LynxDateTime, mapOfComponents: LynxMap): LynxDate = {
    LynxDate.parse(Map("unitStr" -> unitStr, "dateValue" -> dateValue, "mapOfComponents" -> mapOfComponents))
  }

  @LynxProcedure(name = "date.truncate")
  def truncate(unitStr: LynxString, dateValue: LynxDateTime): LynxDate = {
    LynxDate.parse(Map("unitStr" -> unitStr, "dateValue" -> dateValue))
  }

  @LynxProcedure(name = "date.truncate")
  def truncate(): LynxDate = {
    LynxDate.now()
  }

  @LynxProcedure(name = "datetime.transaction")
  def datetime_Transaction(): LynxDateTime = {
    LynxDateTime.now()
  }

  @LynxProcedure(name = "datetime.statement")
  def datetime_Statement(): LynxDateTime = {
    LynxDateTime.now()
  }

  @LynxProcedure(name = "datetime.realtime")
  def datetime_Realtime(): LynxDateTime = {
    LynxDateTime.now()
  }
}

