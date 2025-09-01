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

  def executable(profile: Boolean = false, writer: Option[CSVWriter] = None)(implicit db: VirtualTestBase, gold: GoldVirtualTestBase): Executable = new Executable {
    override def execute(): Unit = {
      var correct = false
      println(s"Test: $id\n Image: $url \n question: $question\n query: ${withPrefix}\n answer: $answer")
//      try {
      val res = db.runner.run(withPrefix, Map.empty, profile = profile).records().map(_.get(0).get).toList.headOption
      val should = answer.toString
      println(s"Should be: $should, but get: ${res.getOrElse(None)}")
      correct = res.map(_.toString) == Some(should)
//      } catch {
//        case e => println(e)
//      } finally {
      writer.foreach{
        _.writeRow(image_index.toString :: question_index.toString :: template_filename :: correct.toString :: Nil)
      }
      if (!correct) {gold.runner.run(withPrefix, Map.empty, profile = true).cache()}
      assert(correct)
//      }
    }
  }

  def eval(writer: Option[CSVWriter] = None)(implicit db: VirtualTestBase): Unit = {
    var correct = false
    db.runner.compile(withPrefix)
    val time0 = System.nanoTime()
    try {
      val res = db.runner.run(withPrefix, Map.empty, profile = false).records().map(_.get(0).get).toList.headOption
      val should = answer.toString
      correct = res.map(_.toString) == Some(should)
    } catch {
      case e => println(e)
    } finally {
      val time = System.nanoTime() - time0
      writer.foreach{
        _.writeRow(image_index.toString
          :: question_index.toString
          :: template_filename
          :: correct.toString
          :: time
          :: Nil)
      }
    }
  }
}

class Clevr1000 {
  implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val db: VirtualTestBase = new VirtualTestBase()

  val clevr_1000_small: List[File] = new java.io.File("datasets/CLEVR1000FULL").listFiles().filterNot(_.getName.startsWith(".")).sortBy(_.getName.substring(10,16)).toList

  def initDB(take: Int = 1000)(implicit db: VirtualTestBase): Unit = {
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
    val jsonContent = Source.fromFile("CLEVR_questions.json").mkString
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

  @Test
  def evaluation(): Unit = {
    val out = "eval_questions_local_device.csv"
    val writer = CSVWriter.open(out)
    var num = 0
    val all = questions.size
    initDB(1000)
    questions.foreach { question =>
      question.eval(Option(writer))
      num += 1
      if (num % 100 == 0) println(s"[$num/$all]")
    }
  }

  @TestFactory
  def testQuestions(): java.util.Collection[DynamicTest] = {
    val PROFILE = true
    val out = "eval_questions.csv"
    val writer = CSVWriter.open(out)
    val gold: GoldVirtualTestBase = new GoldVirtualTestBase()
    initDB(1000)
    initDB(1000)(gold)
    questions.filter(_.template_filename=="comparison.json")
      .take(200)
//      .sortBy(_.template_filename)
      .map{ question =>DynamicTest.dynamicTest(question.id, question.executable(profile = PROFILE)(db, gold))}
      .asJavaCollection
  }

  @Test
  def temp(): Unit = {
    initDB(2)
    singleRun(
      """
        |MATCH (i:Image{image_index: 1})~[:contains]~~<objects> WITH i,collect(objects) AS objects MATCH <o{color:'green',material:'metal'}>~[:right]~~<o2:sphere{size:'large'}>~[:left]~~<o3:sphere{color:'purple'}> WHERE o IN objects AND o2 IN objects AND o3 IN objects RETURN count(o3) AS counto3
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
    db.runner.compile(query)
    db.runner.run(query, Map.empty, profile = false).show()
  }

  def toCypher(query: String): String = {
    query.replaceAll("<([^>]+)>", "($1)")
      .replaceAll("~\\[([^\\[\\]]+)\\]~~", "-[$1]->")
      .replaceAll("~~\\[([^\\[\\]]+)\\]~", "<-[$1]-")
  }

}


