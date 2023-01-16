package org.grapheco.lynx.procedure

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.{LynxInteger, LynxNumber, LynxString}
import org.grapheco.lynx.types.time._
import org.grapheco.lynx.util._

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
      case LynxMap(v) => LynxDateTime.parse(v)
    }
  }


  @LynxProcedure(name = "datetime")
  def datetime(): LynxDateTime = {
    LynxDateTime.now()
  }


  //  @LynxProcedure(name = "datetime")
  //  def datetime(): LynxInteger = {
  //    LynxInteger(LynxDateTime.now().epochMillis)
  //  }

  @LynxProcedure(name = "localdatetime")
  def localDateTime(inputs: LynxValue): LynxLocalDateTime = {
    inputs match {
      case LynxString(v) => LynxLocalDateTime.parse(v)
      case LynxMap(v) => LynxLocalDateTime.parse(v).asInstanceOf[LynxLocalDateTime]
    }
  }

  @LynxProcedure(name = "localdatetime")
  def localDatetime(): LynxLocalDateTime = {
    LynxLocalDateTime.now()
  }

  @LynxProcedure(name = "localdatetime.statement")
  def localDateTime_Statement(): LynxLocalDateTime = {
    LynxLocalDateTime.now()
  }

  @LynxProcedure(name = "localdatetime.realtime")
  def localDateTime_realtime(): LynxLocalDateTime = {
    LynxLocalDateTime.now()
  }

  @LynxProcedure(name = "localdatetime.realtime")
  def localdatetime_realtime(inputs: LynxValue): LynxLocalDateTime = {
    LynxLocalDateTime.parse(Map("timezone" -> inputs)).asInstanceOf[LynxLocalDateTime]
  }

  @LynxProcedure(name = "localdatetime.truncate")
  def localdatetimeTruncate(): LynxDate = {
    LynxDate.now()
  }

  @LynxProcedure(name = "localdatetime.truncate")
  def localdatetimeTruncate(unitStr: LynxString, dateValue: LynxDateTime, mapOfComponents: LynxMap): LynxLocalDateTime = {
    LynxLocalDateTime.parse(Map("unitStr" -> unitStr, "dateValue" -> dateValue, "mapOfComponents" -> mapOfComponents))
  }

  @LynxProcedure(name = "localdatetime.truncate")
  def localdatetimeTruncate(unitStr: LynxString, dateValue: LynxDateTime): LynxLocalDateTime = {
    LynxLocalDateTime.parse(Map("unitStr" -> unitStr, "dateValue" -> dateValue))
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

  @LynxProcedure(name = "time.transaction")
  def timeTransaction(): LynxTime = {
    //TODO
    LynxTime.now()
  }

  @LynxProcedure(name = "time.realtime")
  def timeRealtime(): LynxTime = {
    //TODO
    LynxTime.now()
  }

  @LynxProcedure(name = "time.statement")
  def timeStatement(): LynxTime = {
    //TODO
    LynxTime.now()
  }

  @LynxProcedure(name = "time.truncate")
  def timeTruncate(unitStr: LynxString, dateValue: LynxTime, mapOfComponents: LynxMap): LynxTime = {
    LynxTime.parse(Map("unitStr" -> unitStr, "timeValue" -> dateValue, "mapOfComponents" -> mapOfComponents))
  }

  @LynxProcedure(name = "time.truncate")
  def timeTruncate(unitStr: LynxString, dateValue: LynxTime): LynxTime = {
    LynxTime.parse(Map("unitStr" -> unitStr, "timeValue" -> dateValue))
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

  @LynxProcedure(name = "localtime.realtime")
  def localtimeRealtime(): LynxLocalTime = {
    //TODO
    LynxLocalTime.now()
  }

  @LynxProcedure(name = "localtime.transaction")
  def localtimeTransaction(): LynxLocalTime = {
    //TODO
    LynxLocalTime.now()
  }

  @LynxProcedure(name = "localtime.statement")
  def localtimeStatement(): LynxLocalTime = {
    //TODO
    LynxLocalTime.now()
  }

  @LynxProcedure(name = "localtime.statement")
  def localtimeStatement(input: LynxValue): LynxLocalTime = {
    //TODO
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
      case LynxString(v) => LynxDuration.parse(v,false)
      case LynxMap(v) => LynxDuration.parse(v.asInstanceOf[Map[String, LynxNumber]].mapValues(_.number.doubleValue()))
    }
  }

  @LynxProcedure(name = "duration.between")
  def durationBetween(date_1: LynxValue,date_2: LynxValue): LynxDuration = {
    LynxDuration.between(date_1,date_2)
  }

  @LynxProcedure(name = "duration.inDays")
  def durationBetweenDays(date_1: LynxValue, date_2: LynxValue): LynxDuration = {
    LynxDuration.betweenDays(date_1, date_2)
  }

  @LynxProcedure(name = "duration.inMonths")
  def durationBetweenMonths(date_1: LynxValue, date_2: LynxValue): LynxDuration = {
    LynxDuration.betweenMonths(date_1, date_2)
  }

  @LynxProcedure(name = "duration.inSeconds")
  def durationBetweenSeconds(date_1: LynxValue, date_2: LynxValue): LynxDuration = {
    LynxDuration.betweenSeconds(date_1, date_2)
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
  def dateTruncate(unitStr: LynxString, dateValue: LynxValue, mapOfComponents: LynxMap): LynxDate = {
    LynxDate.parse(Map("unitStr" -> unitStr, "dateValue" -> dateValue, "mapOfComponents" -> mapOfComponents))
  }

  @LynxProcedure(name = "date.truncate")
  def dateTruncate(unitStr: LynxString, dateValue: LynxValue): LynxDate = {
    LynxDate.parse(Map("unitStr" -> unitStr, "dateValue" -> dateValue))
  }


  @LynxProcedure(name = "datetime.truncate")
  def datetimeTruncate(): LynxDate = {
    LynxDate.now()
  }

  @LynxProcedure(name = "datetime.truncate")
  def datetimeTruncate(unitStr: LynxString, dateValue: LynxDateTime, mapOfComponents: LynxMap): LynxDateTime = {
    LynxDateTime.parse(Map("unitStr" -> unitStr, "dateValue" -> dateValue, "mapOfComponents" -> mapOfComponents))
  }

  @LynxProcedure(name = "datetime.truncate")
  def datetimeTruncate(unitStr: LynxString, dateValue: LynxDateTime): LynxDateTime = {
    LynxDateTime.parse(Map("unitStr" -> unitStr, "dateValue" -> dateValue))
  }

  @LynxProcedure(name = "date.truncate")
  def dateTruncate(): LynxDate = {
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

