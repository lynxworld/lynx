package org.grapheco.lynx.types.time

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.HasProperty
import org.grapheco.lynx.util.LynxTemporalParser.{assureBetween, assureContains}

import scala.util.matching.Regex

trait LynxComponentTime extends HasProperty {

  //Integer 0 to 23
  var hour: Int
  //Integer 0 to 59
  var minute: Int
  //Integer 0 to 59
  var second: Int
  //Integer 0 to 999
  var millisecond: Int
  //Integer 0 to 999999
  var microsecond: Int
  //Integer 0 to 999999999
  var nanosecond: Int
  //Integer 0 to 999999999
  var fraction: Int


}

object LynxComponentTime {

  def getHourMinuteSecond(map: Map[String, Any], requiredHasDay: Boolean): (Int, Int, Int) = {

    val hour: Int = map.get("hour").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }.getOrElse(
      map.get("datetime").orNull match {
        case LynxDateTime(v) => v.getHour
        case null =>
          //          assureContains(map, "day")
          0
      })

    val minute: Int = map.get("minute").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }.getOrElse(
      map.get("datetime").orNull match {
        case LynxDateTime(v) => v.getMinute
        case null =>
          0
      })

    val second: Int = map.get("second").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }.getOrElse(
      map.get("datetime").orNull match {
        case LynxDateTime(v) => v.getSecond
        case null =>
          0
      })

    assureBetween(hour, 0, 23, "hour")
    assureBetween(minute, 0, 59, "minute")
    assureBetween(second, 0, 59, "second")
    (hour, minute, second)
  }

  def getNanosecond(map: Map[String, Any], requiredHasSecond: Boolean): Int = {

    if (requiredHasSecond && (map.contains("millisecond") && map.contains("microsecond") && map.contains("nanosecond"))) {
      assureContains(map, "second")
    }

    val millisecond = map.get("millisecond").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }.getOrElse(0)

    val microsecond = map.get("microsecond").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }.getOrElse(0)

    val nanosecond = map.get("nanosecond").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }.getOrElse(0)

    assureBetween(millisecond, 0, 999, "millisecond")

    millisecond match {
      case 0 =>
        microsecond match {
          case 0 => assureBetween(nanosecond, 0, 999999999, "nanosecond")
          case _ =>
            assureBetween(microsecond, 1, 999999, "microsecond")
            assureBetween(nanosecond, 0, 999, "nanosecond")
        }
      case _ =>
        microsecond match {
          case 0 => assureBetween(nanosecond, 0, 999999, "nanosecond")
          case _ =>
            assureBetween(microsecond, 0, 999, "microsecond")
            assureBetween(nanosecond, 0, 999, "nanosecond")
        }
    }

    (millisecond * Math.pow(10, 6) + microsecond * Math.pow(10, 3) + nanosecond).toInt
  }

  def getHourMinuteSecond(timeStr: String): (Int, Int, Int, Int) = {
    val HH_MM_SS_s: Regex = "^([0-9]{2}):([0-9]{2}):([0-9]{2}).([0-9]{3,9})$".r
    val HHMMSS_s: Regex = "^([0-9]{2})([0-9]{2})([0-9]{2}).([0-9]{3,9})$".r
    val HH_MM_SS: Regex = "^([0-9]{2}):([0-9]{2}):([0-9]{2})$".r
    val HHMMSS: Regex = "^([0-9]{2})([0-9]{2})([0-9]{2})$".r
    val HH_MM: Regex = "^([0-9]{2}):([0-9]{2})$".r
    val HHMM: Regex = "^([0-9]{2})([0-9]{2})$".r
    val HH: Regex = "^([0-9]{2})$".r
    timeStr match {
      case HH_MM_SS_s(hour, minute, second, fraction) => (hour.toInt, minute.toInt, second.toInt, fraction.toInt * Math.pow(10, 9 - fraction.length).toInt)
      case HHMMSS_s(hour, minute, second, fraction) => (hour.toInt, minute.toInt, second.toInt, fraction.toInt * Math.pow(10, 9 - fraction.length).toInt)
      case HH_MM_SS(hour, minute, second) => (hour.toInt, minute.toInt, second.toInt, 0)
      case HHMMSS(hour, minute, second) => (hour.toInt, minute.toInt, second.toInt, 0)
      case HH_MM(hour, minute) => (hour.toInt, minute.toInt, 0, 0)
      case HHMM(hour, minute) => (hour.toInt, minute.toInt, 0, 0)
      case HH(hour) => (hour.toInt, 0, 0, 0)
    }
  }

  def truncateTime(map: Map[String, Any]): (Int, Int, Int, Int) = {
    val time = map.get("timeValue").orNull match {
      case v: LynxTime => v
      case null => map.getOrElse("dateValue", 0).asInstanceOf[LynxDateTime]
    }

    var (hour: Int, minute: Int, second: Int, nanosecond: Int, flag: Int) = map("unitStr") match {
      case LynxString("day") => (0, 0, 0, 0, 0)
      case LynxString("hour") => (time.hour, 0, 0, 0, 1)
      case LynxString("minute") => (time.hour, time.minute, 0, 0, 2)
      case LynxString("second") => (time.hour, time.minute, time.second, 0, 3)
      case LynxString("millisecond") => (time.hour, time.minute, time.second, time.millisecond * Math.pow(10, 6).toInt, 4)
      case LynxString("microsecond") => (time.hour, time.minute, time.second, (time.millisecond * Math.pow(10, 6) + time.microsecond * Math.pow(10, 3)).toInt, 5)
      case _ => (0, 0, 0, 0, 0)
    }
    val componentsMap = map.getOrElse("mapOfComponents", null) match {
      case LynxMap(v) => v match {
        case v: Map[String, LynxValue] => v
      }
      case null => return (hour, minute, second, nanosecond)
    }
    val componentList = componentsMap.keys match {
      case v: List[LynxString] => v
      case v: List[String] => v
      case v: Iterable[String] => v.toList
      case v: Iterable[LynxString] => v.toList
    }
    val componentsValue = componentsMap.values match {
      case v: List[LynxInteger] => v
      case v: Iterable[Any] => v.toList
    }

    var millisecond = ((nanosecond * Math.pow(0.1, 6)) * Math.pow(1, 6)).toInt
    var nanosecond_1 = (nanosecond % Math.pow(0.1, 3)).toInt
    var microsecond = nanosecond - nanosecond_1 - millisecond

    for (i <- componentList.indices) {

      if (componentList(i).equals("hour") && (flag < 1)) hour = componentsValue(i).value match {
        case v: Int => v
        case v: Long => v.toInt
      }
      if (componentList(i).equals("minute") && (flag < 2)) minute = componentsValue(i).value match {
        case v: Int => v
        case v: Long => v.toInt
      }
      if (componentList(i).equals("second") && (flag < 3)) second = componentsValue(i).value match {
        case v: Int => v
        case v: Long => v.toInt
      }
      if (componentList(i).equals("millisecond") && (flag < 4))
        millisecond = ((componentsMap.getOrElse("millisecond", 0) match {
          case v: Int => v
          case LynxInteger(v) => v
          case v: Long => v
        }) * Math.pow(10, 6)).toInt
      if (componentList(i).equals("microsecond") && (flag < 5))
        microsecond = ((componentsMap.getOrElse("microsecond", 0) match {
          case v: Int => v
          case LynxInteger(v) => v
          case v: Long => v
        }) * Math.pow(10, 3)).toInt
      if (componentList(i).equals("nanosecond") && (flag < 5))
        nanosecond_1 = componentsMap.getOrElse("nanosecond", 0) match {
          case v: Int => v
          case LynxInteger(v) => v.toInt
          case v: Long => v.toInt
        }
    }


    (hour, minute, second, millisecond + microsecond + nanosecond_1)
  }
}

