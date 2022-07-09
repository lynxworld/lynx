package org.grapheco.LDBC

import com.github.tototoshi.csv.{CSVFormat, CSVReader, DefaultCSVFormat, Quoting}
import org.grapheco.lynx.{LynxResult, TestBase}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxInteger, LynxNull, LynxString}
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.lynx.types.time.LynxDate
import org.junit.Test

import java.io.File
import java.time.LocalDate

/**
 * @ClassName LDBCTestBase
 * @Description
 * @Author Hu Chuan
 * @Date 2022/6/28
 * @Version 0.1
 */
class LDBCTestBase extends TestBase {

  val CSV_FORMAT: DefaultCSVFormat = new DefaultCSVFormat{
    override val delimiter: Char = '|'
  }

  def run(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult = {
    this.runOnDemoGraph(query, param)
  }

  def loadLDBC(path: String): Unit = {
    val nodeFiles = new File(path + "/nodes")
    val relFiles = new File(path + "/relations")
    nodeFiles.listFiles().foreach{ f =>
      val data = CSVReader.open(f)(CSV_FORMAT).iterator
      val header = data.next()
      importNode(header.toArray, data)
    }
    relFiles.listFiles().foreach{ f =>
      val data = CSVReader.open(f)(CSV_FORMAT).iterator
      val header = data.next()
      importRelation(header.toArray, data)
    }
    this.runOnDemoGraph("match(n:Comment) set n:Message")
    this.runOnDemoGraph("match(n:Post) set n:Message")
  }

  private def parse(header: Array[String]): Seq[(Int, String, String => LynxValue)]  ={
    header.zipWithIndex.map{ case (format, index) =>
      val name = format.split(':').head
      val transFunc: String => LynxValue = (string: String) => format.split(':').lift(1).getOrElse("string") match {
        case "ID" => LynxString(string)
        case "IGNORE" => LynxNull
        case "LABEL" => LynxNull
        case "TYPE" => LynxNull
        case "START_ID" => LynxNull
        case "END_ID" => LynxNull
        case "int" => LynxInteger(string.toLong)
        case "Date" => LynxDate(LocalDate.parse(string))
        case "string" => LynxString.apply(string)
        case "string[]" => LynxList(string.split(';').toList.map(LynxString))
      }
      (index, name, transFunc)
    }
  }

  private def importNode(header: Array[String], data: Iterator[Seq[String]]): Unit ={
    val idIndex = header.indexWhere(_.contains(":ID"))
    val labelIndex = header.indexWhere(_.contains(":LABEL"))
    if (idIndex <0 || labelIndex <0) throw new Exception(":ID or :LABEL not found.") // check
    val properties = parse(header)
    data.foreach{ d =>
      this.all_nodes += TestNode(
        TestId(d(idIndex).toLong),
        Seq(LynxNodeLabel(d(labelIndex))),
        properties.filterNot{case (i, _, _) => i == labelIndex}
          .map{ case (i, str, stringToValue) => (d(i), str, stringToValue)}
          .filterNot(_._1.equals(""))
          .map{ case (data, str, stringToValue) => LynxPropertyKey(str) -> stringToValue(data)}.toMap
      )
    }
  }

  private def importRelation(header: Array[String], data: Iterator[Seq[String]]): Unit ={
    val idIndex = header.indexWhere(_.contains(":IGNORE"))
    val typeIndex = header.indexWhere(_.contains(":TYPE"))
    val startIndex = header.indexWhere(_.contains(":START_ID"))
    val endIndex = header.indexWhere(_.contains(":END_ID"))
    if (idIndex <0 || typeIndex <0 || startIndex <0 || endIndex <0) throw new Exception(":ID, :START_ID, :END_ID or :TYPE not found.") // check
    val properties = parse(header)
    data.foreach{ d =>
      this.all_rels += TestRelationship(
        TestId(d(idIndex).toLong),
        TestId(d(startIndex).toLong),
        TestId(d(endIndex).toLong),
        Some(LynxRelationshipType(d(typeIndex))),
        properties.filterNot{case (i, _, _) => List(idIndex, typeIndex, startIndex, endIndex).contains(i)}
          .map{ case (i, str, stringToValue) => (d(i), str, stringToValue)}
          .filterNot(_._1.equals(""))
          .map{ case (data, str, stringToValue) => LynxPropertyKey(str) -> stringToValue(data)}.toMap
      )
    }
  }
}
