package org.grapheco.lynx.types.time

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.traits.HasProperty
import org.grapheco.lynx.util.LynxTemporalParser.{assureBetween, assureContains}

import scala.util.matching.Regex
import java.time.{LocalDateTime, ZonedDateTime}
import java.time.LocalDate
import java.util.{Calendar, GregorianCalendar}

trait LynxComponentDate extends HasProperty {

  //Integer at least 4 digits
  var year: Int
  //Integer 1 to 4
  var quarter: Int
  //Integer 1 to 12
  var month: Int
  //Integer 1 to 53
  var week: Int
  //Integer at least 4 digits
  var weekYear: Int
  //Integer 1 to 92
  var dayOfQuarter: Int
  //Integer 1 to 92
  var quarterDay: Int
  //Integer 1 to 31
  var day: Int
  //Integer 1 to 366
  var ordinalDay: Int
  //Integer 1 to 7
  var dayOfWeek: Int
  //Integer 1 to 7
  var weekDay: Int

}

object LynxComponentDate {

  def getYearMonthDay(map: Map[String, Any]): (Int, Int, Int) = {

    val year: Int = map.get("year").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
      case v: Int => v
    }.getOrElse(
      map.get("datetime").orNull match {
        case LynxDateTime(v) => v.getYear
        case null => 0
      }
    )

    val month: Int = map.get("month").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
      case v: Int => v
    }.getOrElse(map.getOrElse("datetime", null) match {
      case LynxDateTime(v) => v.getMonthValue
      case null =>
        if (map.contains("month")) {
          assureContains(map, "year")
        }
        1
    })

    //    if (map.contains("day")) {
    //      assureContains(map, "month")
    //    }
    val day: Int = map.get("day").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
      case v: Int => v
    }.getOrElse(
      map.get("datetime").orNull match {
        case LynxDateTime(v) => v.getDayOfMonth
        case null =>
          if (map.contains("day")) {
            assureContains(map, "month")
          }
          1
      })

    assureBetween(month, 1, 12, "month")
    assureBetween(day, 1, 31, "day")
    (year, month, day)
  }

  def transformYearWeekDay(map: Map[String, Any]): (Int, Int, Int) = {

    val year: Int = map.get("year").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
      case v: Int => v
    }.getOrElse(0)

    if (map.contains("week")) {
      assureContains(map, "year")
    }
    val week: Int = map.get("week").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
      case v: Int => v
    }.getOrElse(1)

    if (map.contains("dayOfWeek")) {
      assureContains(map, "week")
    }
    val day: Int = map.get("dayOfWeek").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
      case v: Int => v
    }.getOrElse(1)

    assureBetween(week, 1, 53, "week")
    assureBetween(day, 1, 7, "dayOfWeek")
    val calendar = new GregorianCalendar()
    calendar.clear()
    calendar.set(Calendar.YEAR, year)
    calendar.set(Calendar.WEEK_OF_YEAR, week)
    calendar.set(Calendar.DAY_OF_WEEK, day)
    (calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH) + 1)
  }

  def transformYearMonthDay(map: Map[String, Any]): (Int, Int, Int) = {

    val year: Int = map.get("year").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
      case v: Int => v
    }.getOrElse(0)

    if (map.contains("month")) {
      assureContains(map, "year")
    }
    val month: Int = map.get("month").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
      case v: Int => v
    }.getOrElse(1)

    if (map.contains("dayOfMonth")) {
      assureContains(map, "month")
    }
    val day: Int = map.get("dayOfMonth").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
      case v: Int => v
    }.getOrElse(1)

    assureBetween(month, 1, 12, "month")
    assureBetween(day, 1, 31, "dayOfMonth")
    val calendar = new GregorianCalendar()
    calendar.clear()
    calendar.set(Calendar.YEAR, year)
    calendar.set(Calendar.MONTH, month)
    calendar.set(Calendar.DAY_OF_MONTH, day)
    (calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH) + 1)
  }

  def transformYearQuarterDay(map: Map[String, Any]): (Int, Int, Int) = {

    val year: Int = map.get("year").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
      case v: Int => v
    }.getOrElse(0)

    if (map.contains("quarter")) {
      assureContains(map, "year")
    }
    val quarter: Int = map.get("quarter").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
      case v: Int => v
    }.getOrElse(1)

    if (map.contains("dayOfQuarter")) {
      assureContains(map, "quarter")
    }
    val day: Int = map.get("dayOfQuarter").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
      case v: Int => v
    }.getOrElse(1)

    assureBetween(quarter, 1, 4, "quarter")
    assureBetween(day, 1, 92, "dayOfQuarter")

    val calendar = new GregorianCalendar()
    calendar.clear()
    calendar.set(Calendar.YEAR, year)
    calendar.set(Calendar.MONTH, quarter * 3 - 3)
    calendar.set(Calendar.DAY_OF_MONTH, day)

    (calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH))
  }

  def transformYearOrdinalDay(map: Map[String, Any]): (Int, Int, Int) = {

    val year: Int = map.get("year").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
      case v: Int => v
    }.getOrElse(0)

    if (map.contains("ordinalDay")) {
      assureContains(map, "year")
    }
    val ordinalDay: Int = map.get("ordinalDay").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
      case v: Int => v
    }.getOrElse(1)

    assureBetween(ordinalDay, 1, 366, "ordinalDay")
    val calendar = new GregorianCalendar()
    calendar.set(Calendar.YEAR, year)
    calendar.set(Calendar.DAY_OF_YEAR, ordinalDay)
    (calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH))
  }

  def transformDate(map: Map[String, Any]): (Int, Int, Int) = {

    var (year, month, day) = map.getOrElse("date", 0) match {
      case v: LocalDate => (v.getYear, v.getMonthValue, v.getDayOfMonth)
      case v: ZonedDateTime => (v.getYear, v.getMonthValue, v.getDayOfMonth)
      case v: LocalDateTime => (v.getYear, v.getMonthValue, v.getDayOfMonth)
      case v: LynxDate => (v.year, v.month, v.day)
      case v: LynxLocalDateTime => (v.year, v.month, v.day)
      case v: LynxDateTime => (v.year, v.month, v.day)
    }
    if (map.contains("day")) {
      assureContains(map, "date")
    }
    day = map.get("day").map {
      case v: LynxInteger => v.value.toInt
      case v: Long => v.toInt
    }.getOrElse(day)

    (year, month, day)
  }

  def getYearMonthDay(dateStr: String): (Int, Int, Int) = {
    val YYYY_MM_DD: Regex = "^([0-9]{4})-([0-9]{2})-([0-9]{2})$".r
    val YYYYMMDD: Regex = "^([0-9]{4})([0-9]{2})([0-9]{2})$".r
    // 识别 'YYYY/MM/DD'
    val YYYY_SLASH_MM_SLASH_DD: Regex = "^([0-9]{4})/([0-9]{2})/([0-9]{2})$".r
    val YYYY_MM: Regex = "^([0-9]{4})-([0-9]{2})$".r
    val YYYYMM: Regex = "^([0-9]{4})([0-9]{2})$".r
    val YYYY_Www_D: Regex = "^[-,+]?([0-9]{4})-W([0-9]{2})-([1-7]{1})$".r
    val YYYYWwwD: Regex = "^([0-9]{4})W([0-9]{2})([1-7]{1})$".r
    val YYYY_Www: Regex = "^([0-9]{4})-W([0-9]{2})$".r
    val YYYYWww: Regex = "^([0-9]{4})W([0-9]{2})$".r
    val YYYY_Qq_DD: Regex = "^([0-9]{4})-Q([0-9]{1})-([0-9]{2})$".r
    val YYYYQqDD: Regex = "^([0-9]{4})Q([0-9]{1})([0-9]{2})$".r
    val YYYY_Qq: Regex = "^([0-9]{4})-Q([0-9]{1})$".r
    val YYYYQq: Regex = "^([0-9]{4})Q([0-9]{1})$".r
    val YYYY_DDD: Regex = "^([0-9]{4})-([0-9]{3})$".r
    val YYYYDDD: Regex = "^([0-9]{4})([0-9]{3})$".r
    val YYYY: Regex = "^([0-9]{4})$".r

    dateStr match {
      case YYYY_MM_DD(year, month, day) => (year.toInt, month.toInt, day.toInt)
      case YYYYMMDD(year, month, day) => (year.toInt, month.toInt, day.toInt)
      case YYYY_SLASH_MM_SLASH_DD(year, month, day) => (year.toInt, month.toInt, day.toInt)
      case YYYY_MM(year, month) => (year.toInt, month.toInt, 1)
      case YYYYMM(year, month) => (year.toInt, month.toInt, 1)
      case YYYY_Www_D(year, week, day) => transformYearWeekDay(Map("year" -> year.toInt, "week" -> week.toInt, "dayOfWeek" -> day.toInt))
      case YYYYWwwD(year, week, day) => transformYearWeekDay(Map("year" -> year.toInt, "week" -> week.toInt, "dayOfWeek" -> day.toInt))
      case YYYY_Www(year, week) => transformYearWeekDay(Map("year" -> year.toInt, "week" -> week.toInt, "dayOfWeek" -> 1))
      case YYYYWww(year, week) => transformYearWeekDay(Map("year" -> year.toInt, "week" -> week.toInt, "dayOfWeek" -> 1))
      case YYYY_Qq_DD(year, quarter, day) => transformYearQuarterDay(Map("year" -> year.toInt, "quarter" -> quarter.toInt, "dayOfQuarter" -> day.toInt))
      case YYYYQqDD(year, quarter, day) => transformYearQuarterDay(Map("year" -> year.toInt, "quarter" -> quarter.toInt, "dayOfQuarter" -> day.toInt))
      case YYYY_Qq(year, quarter) => transformYearQuarterDay(Map("year" -> year.toInt, "quarter" -> quarter.toInt, "dayOfQuarter" -> 1))
      case YYYYQq(year, quarter) => transformYearQuarterDay(Map("year" -> year.toInt, "quarter" -> quarter.toInt, "dayOfQuarter" -> 1))
      case YYYY_DDD(year, ordinalDay) => transformYearOrdinalDay(Map("year" -> year.toInt, "ordinalDay" -> ordinalDay.toInt))
      case YYYYDDD(year, ordinalDay) => transformYearOrdinalDay(Map("year" -> year.toInt, "ordinalDay" -> ordinalDay.toInt))
      case YYYY(year) => (year.toInt, 1, 1)
    }
  }

  def truncateDate(map: Map[String, Any]): (Int, Int, Int) = {

    val calendar = Calendar.getInstance()
    calendar.clear()

    //    val date = map.getOrElse("dateValue", 0) match {
    //      case LynxDateTime(v) => v
    //      case LynxDate(v) => v
    //    }
    val (year, month, day): (Int, Int, Int)=map.getOrElse("dateValue", 0) match {
      case LynxDateTime(v) =>(v.getYear,v.getMonthValue-1,v.getDayOfMonth)
      case LynxDate(v) => (v.getYear,v.getMonthValue-1,v.getDayOfMonth)
      case LynxLocalDateTime(v) => (v.getYear,v.getMonthValue-1,v.getDayOfMonth)
    }

    //    val (year, month, day): (Int, Int, Int) = (date.getYear, date.getMonthValue - 1, date.getDayOfMonth)
    calendar.setFirstDayOfWeek(Calendar.MONDAY)
    map("unitStr") match {
      case LynxString("millennium")
      => calendar.set(year - year % 1000, 0, 1)
      case LynxString("century")
      => calendar.set(year - year % 100, 0, 1)
      case LynxString("decade")
      => calendar.set(year - year % 10, 0, 1)
      case LynxString("year")
      => calendar.set(year, 0, 1)
      case LynxString("weekYear")
      => calendar.set(Calendar.YEAR, year)
        calendar.set(Calendar.WEEK_OF_YEAR, year)
        calendar.set(year, 0, 1)
        calendar.set(Calendar.DAY_OF_YEAR, calendar.get(Calendar.DAY_OF_WEEK) match {
          case 1 => 2
          case 2 => 1
          case v => 10 - v.toInt
        })
      case LynxString("quarter")
      => calendar.set(year, month - month % 3, 1)
      case LynxString("month")
      => calendar.set(year, month, 1)
      case LynxString("week")
      => calendar.set(year, month, day)
        calendar.set(Calendar.DAY_OF_WEEK, 1)
      case LynxString("day")
      => calendar.set(year, month, day)
      case _ => calendar.set(year, month, day)
    }
    val componentsMap = map.getOrElse("mapOfComponents", null) match {
      case LynxMap(v) => v match {
        case v: Map[String, LynxValue] => v
      }
      case null => return (calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH))
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
      //      case v: Iterable[LynxString] => v.toList
    }
    var addDay = 0
    for (i <- componentList.indices) {

      if (componentList(i).equals("weekYear") || componentList(i).equals("century") || componentList(i).equals("decade") || componentList(i).equals("year")) {
        calendar.set(Calendar.YEAR, componentsValue(i).value match {
          case v: Int => v
          case v: Long => v.toInt
        })
      }
      if (componentList(i).equals("month")) {
        calendar.set(Calendar.MONTH, componentsValue(i).value match {
          case v: Int => v
          case v: Long => v.toInt
        })
      }
      if (componentList(i).equals("day")) {
        calendar.set(Calendar.DAY_OF_MONTH, componentsValue(i).value match {
          case v: Int => v
          case v: Long => v.toInt
        })
      }
      if (componentList(i).equals("dayOfWeek")) {
        calendar.set(Calendar.WEEK_OF_YEAR, calendar.get(Calendar.WEEK_OF_YEAR))
        calendar.set(Calendar.DAY_OF_WEEK, componentsValue(i).value match {
          case v: Int => v + 1
          case v: Long => v.toInt + 1
        })
      }
    }
    (calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH))
  }
}
