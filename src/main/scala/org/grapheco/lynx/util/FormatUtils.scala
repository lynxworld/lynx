package org.grapheco.lynx.util

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.commons.lang3.StringUtils
import org.grapheco.lynx.types.LynxValue

object FormatUtils {
//  def format(x: Any, nullString: String = "(null)"): String = {
//    x match {
//      case null => nullString
//      case None => nullString
//      case Some(m) => format(m, nullString);
//      case date: Date => new SimpleDateFormat("yyyy-MM-dd HH:mm:SS").format(date)
//      case _ => x.toString
//    }
//  }

  def printTable(columns: Seq[String], data: Seq[Seq[String]]) = {
//    val formattedColumns: Seq[String] = columns.map(format(_, nullString))
//    val formattedData: Seq[Seq[String]] = data.map(_.map(format(_, nullString)))

    val sb = new StringBuilder
    val numCols = columns.length

    //width>=3
    val colWidths = Array.fill(numCols)(3)

    //width of each column
    for (row ← data :+ columns) {
      for ((cell, i) ← row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }

    // Create SeparateLine
    colWidths.map("═" * _).addString(sb, "╔", "╤", "╗\n")

    // column names
    columns.zipWithIndex.map {
      case (cell, i) ⇒
        StringUtils.rightPad(cell, colWidths(i))
    }.addString(sb, "║", "│", "║\n")

    colWidths.map("═" * _).addString(sb, "╠", "╪", "╣\n")

    // data
    data.foreach {
      _.zipWithIndex.map {
        case (cell, i) ⇒
          StringUtils.rightPad(cell, colWidths(i))
      }.addString(sb, "║", "│", "║\n")
    }

    colWidths.map("═" * _).addString(sb, "╚", "╧", "╝\n")

    println(sb)
  }


  def convertPatternComprehension(query: String): String = {
    if (!containsPatternComprehension(query)) {
      query
    } else {
      val (matchPart, returnPart) = splitCypherQuery(query)
      val (leftPart, rightPart, asPart) = extractReturnParts(returnPart)
      val node = extractNode(matchPart)
      "MATCH " + matchPart + "\n" +
      "WITH " + node.get + "\n" +
      "MATCH " + leftPart + "\n" +
      "RETURN " + rightPart + " AS " + asPart
    }
  }

  def containsPatternComprehension(query: String): Boolean = {
    val pattern = """RETURN\s+\[(.*)\]\s+AS\s+\w+""".r
    val matches = pattern.findAllMatchIn(query)

    for (matchResult <- matches) {
      val fullPattern = matchResult.group(1)
      val subQueryPattern = """\((.*?)-\[(\w+:\w+)\]->\((.*?)\) WHERE (.*?):(\w+)""".r
      val subQueryMatch = subQueryPattern.findFirstMatchIn(fullPattern)

      if (subQueryMatch.isDefined) {
        return true
      }
    }

    false
  }

  def extractNode(query: String): Option[String] = {
    val pattern = "\\((\\w+):".r

    pattern.findFirstMatchIn(query).map(_.group(1))
  }

  def extractReturnParts(query: String): (String, String, String) = {
    val pattern1 = "\\[(.*?)\\|".r
    val pattern2 = "\\|(.*?)\\]".r
    val pattern3 = "AS (\\w+)".r

    val match1 = pattern1.findFirstMatchIn(query).map(_.group(1)).getOrElse("")
    val match2 = pattern2.findFirstMatchIn(query).map(_.group(1)).getOrElse("")
    val match3 = pattern3.findFirstMatchIn(query).map(_.group(1)).getOrElse("")

    (match1, match2, match3)
  }

  def splitCypherQuery(query: String): (String, String) = {
    val matchPattern = "MATCH (.+?)\n".r
    val returnPattern = "RETURN (.+)".r

    val matchPart = matchPattern.findFirstMatchIn(query).map(_.group(1)).getOrElse("")
    val returnPart = returnPattern.findFirstMatchIn(query).map(_.group(1)).getOrElse("")

    (matchPart, returnPart)
  }
}
