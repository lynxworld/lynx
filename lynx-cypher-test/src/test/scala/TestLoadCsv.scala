import java.io.{File, PrintWriter}

import org.junit.Test

import scala.collection.mutable.ArrayBuffer

class TestLoadCsv {
  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  val testBase = new TestBase(nodesBuffer, relsBuffer)

  val csvFilePath = "./csvDataFile.csv"
  def createCsvFile(withHeader: Boolean = false): Unit ={
    val writer = new PrintWriter(new File(csvFilePath))
    if (withHeader) writer.println("Id,Name,Year")
    writer.println("1,ABBA,1992")
    writer.println("2,Roxette,1986")
    writer.println("3,Europe,1979")
    writer.println("4,The Cardigans,1992")
    writer.flush()
    writer.close()
  }
  def createCsvWithDelimiter(): Unit ={
    val writer = new PrintWriter(new File(csvFilePath))
    writer.println("1;ABBA;1992")
    writer.println("2;Roxette;1986")
    writer.println("3;Europe;1979")
    writer.println("4;The Cardigans;1992")
    writer.flush()
    writer.close()
  }
  @Test
  def importDataFromACsvFile(): Unit ={
    createCsvFile()
    val res = testBase.runOnDemoGraph(
      """
        |LOAD CSV FROM './csvDataFile.csv' AS line
        |CREATE (:Artist { name: line[1], year: toInteger(line[2])})
        |""".stripMargin).records().toArray
  }
  @Test
  def importDataFromRemoteCsvFile(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |LOAD CSV FROM 'http://data.neo4j.com/bands/artists.csv' AS line
        |CREATE (:Artist { name: line[1], year: toInteger(line[2])})
        |""".stripMargin).records().toArray
  }
  @Test
  def importDataFromACsvFileContainHeader(): Unit ={
    createCsvFile(true)
    val res = testBase.runOnDemoGraph(
      """
        |LOAD CSV WITH HEADERS FROM '{csv-dir}/artists-with-headers.csv' AS line
        |CREATE (:Artist { name: line.Name, year: toInteger(line.Year)})
        |""".stripMargin).records().toArray
  }
  @Test
  def importDataFromACsvFileWithCustomFieldDelimiter(): Unit ={
    createCsvWithDelimiter()
    val res = testBase.runOnDemoGraph(
      """
        |LOAD CSV FROM '{csv-dir}/artists-fieldterminator.csv' AS line FIELDTERMINATOR ';'
        |CREATE (:Artist { name: line[1], year: toInteger(line[2])})
        |""".stripMargin)
  }
}
