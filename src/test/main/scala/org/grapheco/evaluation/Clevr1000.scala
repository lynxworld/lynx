package org.grapheco.evaluation

import com.github.tototoshi.csv.CSVWriter
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.json4s._
import org.json4s.native.JsonParser
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.{BeforeEach, DynamicTest, Test, TestFactory}
import org.opencypher.v9_0.parser.Query

import java.io.File
import scala.collection.JavaConverters.asJavaCollectionConverter
import scala.io.Source

//image_index, question, query, answer,template_filename,question_family_index,question_index
case class ClevrQuestion( image_index: Int,
                          question: String,
                          query: String,
                          answer: Any,
                          template_filename: String,
                          question_family_index: Int,
                          question_index: Int) {
  val prefix = s"MATCH (i:Image{image_index: $image_index})"
  val url = s"http://10.0.82.214:12306/CLEVR_val_${image_index.toString.reverse.padTo(6,'0').reverse}.png"

  val id = s"$image_index-$question_index($template_filename)"
  val withPrefix: String = query.replace("MATCH (i)", prefix)

  def executable(profile: Boolean = false, writer: Option[CSVWriter] = None)(implicit db: VirtualTestBase): Executable = new Executable {
    override def execute(): Unit = {
      var correct = false
      println(s"Test: $id\n Image: $url \n question: $question\n query: ${withPrefix}\n answer: $answer")
      try {
        val res = db.runner.run(withPrefix, Map.empty, profile = profile).records().map(_.get(0).get).toList.headOption
        val should = answer.toString
        println(s"Should be: $should, but get: ${res.getOrElse(None)}")
        correct = res.map(_.toString) == Some(should)
      } catch {
        case e => println(e)
      } finally {
        writer.foreach{
          _.writeRow(image_index.toString :: question_index.toString :: template_filename :: correct.toString :: Nil)
        }
        assert(correct)
      }
    }
  }
}

class Clevr1000 {
  implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val db: VirtualTestBase = new VirtualTestBase()

  val clevr_1000_small: List[File] = new java.io.File("datasets/CLEVR1000FULL").listFiles().filterNot(_.getName.startsWith(".")).sortBy(_.getName.substring(10,16)).toList

  def initDB(take: Int = 1000): Unit = {
    if (db.all_nodes.isEmpty) {
      clevr_1000_small.take(take).zipWithIndex.map{ case (f,i) =>
        db.all_nodes.put(
          TestId(i),
          TestNode(TestId(i), Seq(LynxNodeLabel("Image")),
            Map(
              LynxPropertyKey("file") -> LynxValue(f.getPath),
              LynxPropertyKey("image_index") -> LynxValue(f.getName.substring(10,16).toInt),
              LynxPropertyKey("id") -> LynxValue(i))
          )
        )
      }
    }
  }

  val questions: List[ClevrQuestion] = {
    val jsonContent = Source.fromFile("/Users/huchuan/Documents/GitHub/clevr-dataset-gen/output/CLEVR_questions.json").mkString
    JsonParser.parse(jsonContent).\("questions").extract[List[ClevrQuestion]]
  }

  @Test
  def parseableTest(): Unit = {
    val testCases = questions
    // statistics how many query can be compiled, save number
    var success = 0
    testCases.foreach{ case ClevrQuestion(image_index, question, query, answer, template_filename, question_family_index, question_index) =>
      try {
        db.runner.compile(query)
        success += 1
      } catch {case e: Exception => println(s"query $query compile failed")}
    }
    println(s"success: $success, total: ${testCases.size}")
  }

  @Test
  def runnableTest(): Unit = {
    val testCases = questions
    // statistics how many query can be compiled, save number
    var success = 0
    testCases.foreach{ case ClevrQuestion(image_index, question, query, answer, template_filename, question_family_index, question_index) =>
      try {
        db.runner.run(query, Map.empty)
        success += 1
      } catch {case e: Exception => println(s"query $query run failed, ${e.getMessage}")}
    }
    println(s"success: $success, total: ${testCases.size}")
  }

  @TestFactory
  def testQuestions(): java.util.Collection[DynamicTest] = {
    val PROFILE = true
    val out = "eval_questions.csv"
    val writer = CSVWriter.open(out)

    initDB(100)
    questions.take(200)
//      .sortBy(_.template_filename)
      .map{ question =>DynamicTest.dynamicTest(question.id, question.executable(profile = PROFILE, writer = Some(writer)))}
      .asJavaCollection
  }

  @Test
  def temp(): Unit = {
    initDB(20)
    singleRun(
      """
        |MATCH (i:Image{image_index: 18})~[:contains]~~<objects> WITH i,collect(objects) AS objects MATCH <o:cylinder{size:'large',color:'red'}>~[:left]~~<o3>~~[:front]~<o2{size:'small',color:'cyan',material:'rubber'}> WHERE o IN objects AND o2 IN objects AND o3 IN objects RETURN o3.material AS material
        |
        |
        |""".stripMargin)
  }

  @Test
  def forAnalyse_Segement(): Unit = {
    initDB(1000)
    val writer = CSVWriter.open(new File("clevr1000.csv"))
    writer.writeRow("image_index" :: "area" :: "box" :: "file" :: Nil)
    var i = 0
    var lastId = ""
    val allResult = db.runner.run(
      """
        |MATCH (i:Image)~[:contains]~~<o>
        |RETURN i.image_index, o.area, o.box, o.file
        |""".stripMargin, Map.empty, profile = false)
    allResult.records().foreach{ record =>
      val image_index = record.get(0).get.toString
      writer.writeRow(image_index ::
        record.get(1).get.toString ::
        record.get(2).get.asInstanceOf[LynxList].v.mkString("-") :: // [1-2-3-4]
        record.get(3).get.toString :: Nil)

      if (image_index != lastId) {
        i += 1
        if (i % 10 == 0) writer.flush()
//        println(s"[$i/1000]")
        lastId = image_index
      }
    }
    writer.flush()
    writer.close()
  }

  @Test
  def forAnalyse(): Unit = {
    initDB(1000)
    val writer = CSVWriter.open(new File("clevr1000.csv"))
    // write header
    writer.writeRow("image_index" :: "area" :: "box" :: "color" :: "size" :: "material" :: "label" :: Nil)
    var i = 0
    val allResult = db.runner.run(
      """
        |MATCH (i:Image)~[:contains]~~<o>
        |RETURN i.image_index, o.area, o.box, o.color, o.size, o.material, labels(o)[0]
        |""".stripMargin, Map.empty, profile = false)
    allResult.records().foreach{ record =>
      writer.writeRow(record.get(0).get.toString ::
          record.get(1).get.toString ::
          record.get(2).get.asInstanceOf[LynxList].v.mkString("-") :: // [1-2-3-4]
          record.get(3).get.toString ::
          record.get(4).get.toString ::
          record.get(5).get.toString ::
          record.get(6).get.toString :: Nil)
      println(s"[$i/1000]")
      if (i % 10 == 0) writer.flush()
      i += 1
    }
    writer.flush()
    writer.close()
  }

  def singleRun(query: String, init: Int = 100): Unit = {
    initDB(init)
    println(toCypher(query))
    db.runner.run(query, Map.empty, profile = true).show()
  }

  def toCypher(query: String): String = {
    query.replaceAll("<([^>]+)>", "($1)")
      .replaceAll("~\\[([^\\[\\]]+)\\]~~", "-[$1]->")
      .replaceAll("~~\\[([^\\[\\]]+)\\]~", "<-[$1]-")
  }

}

class GoldClevr1000 extends Clevr1000 {
  override implicit val db: GoldVirtualTestBase = new GoldVirtualTestBase()

  @TestFactory
  def goldTest(): java.util.Collection[DynamicTest] = {
    initDB(100)
    questions.take(1000)
      .sortBy(_.template_filename)
      .map{ question =>DynamicTest.dynamicTest(question.id, question.executable(profile = false))}
      .asJavaCollection
  }

  @TestFactory
  def goldTemplateTest(): java.util.Collection[DynamicTest] = {
    initDB(100)
    val templateName = "compare_integer.json"
    questions.take(1000)
      .filter(_.template_filename == templateName)
      .sortBy(_.template_filename)
      .map{ question =>DynamicTest.dynamicTest(question.id, question.executable(profile = true))}
      .asJavaCollection
  }

  @Test
  def compare_integer(): Unit = {
    initDB(10)
    db.runner.run(
      """
        |MATCH (i:Image{image_index: 7})~[:contains]~~<objects>
        | WITH collect(objects) AS objects
        | OPTIONAL  MATCH <o:sphere>~[:behind]~~<o2{color:'brown',material:'rubber'}>
        | WHERE o IN objects AND o2 IN objects
        | OPTIONAL  MATCH <o3:cube{size:'large',material:'rubber'}>
        | WHERE o3 IN objects
        | RETURN count(DISTINCT o2)<count(DISTINCT o3)
        |""".stripMargin, Map.empty, profile = true).show()
  }



  @Test
  def compare(): Unit = {
    initDB(10)
    db.runner.run(
      """
        |MATCH (i:Image{image_index: 6})~[:contains]~~<objects> WITH i,collect(objects) AS objects
        | MATCH <o{material:'metal'}>~[:front]~~<o2:cube>
        | WHERE o IN objects AND o2 IN objects
        | RETURN labels(o2)
        |""".stripMargin, Map.empty, profile = true).show()
  }

  @Test
  def single_or(): Unit = singleRun {
    """
      |MATCH (i:Image{image_index: 71})~[:contains]~~<objects:cube> WITH i,collect(objects) AS objects OPTIONAL  MATCH <o{color:'red',material:'rubber'}> WHERE o IN objects WITH objects,o OPTIONAL  MATCH <o2{color:'red'}> WHERE o2 IN objects WITH collect( distinct o) + collect(distinct o2) AS all unwind all as a
      |RETURN count( distinct a)
      |
      |""".stripMargin
  }

  @Test
  def same_relate(): Unit = singleRun {
    """
      |MATCH (i:Image{image_index: 54})~[r:contains]~~<o2>,(i)~[r2:contains]~~<o:sphere{material:'metal'}>
      |WHERE o.color=o2.color
      |return o,o2,r,r2
      |
      |""".stripMargin
  }

  @Test
  def temp2(): Unit = {
    singleRun(
      """
        |MATCH (i:Image{image_index: 9})~[:contains]~~<objects>
        | WITH collect(objects) AS objects
        | OPTIONAL  MATCH <o{size:'large',color:'gray'}>~[:front]~~<o2{size:'small',color:'gray',material:'rubber'}>
        | WHERE o IN objects AND o2 IN objects
        | OPTIONAL  MATCH <o3{size:'small',color:'brown'}>~[:left]~~<o4>
        | WHERE o3 IN objects AND o4 IN objects
        | RETURN count(DISTINCT o2)=count(DISTINCT o4)
        |
        |
        |""".stripMargin)
  }

  @Test
  def oneHop(): Unit = singleRun {
    """
      |MATCH (i:Image{image_index: 4})~[:contains]~~<objects>
      |WITH i,collect(objects) AS objects
      |MATCH <o{color:'purple'}>~[:front]~~<o2{size:'small'}>
      |WHERE o IN objects AND o2 IN objects
      |RETURN o2.color
      |""".stripMargin
  }



}
