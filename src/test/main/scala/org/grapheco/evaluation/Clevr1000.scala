package org.grapheco.evaluation

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.json4s._
import org.json4s.native.JsonParser
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.{BeforeEach, DynamicTest, Test, TestFactory}

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
  val prefix = s"MATCH (i:Image{image_index: $image_index}) WITH i"
  val url = s"http://10.0.82.214:12306/CLEVR_val_${image_index.toString.reverse.padTo(6,'0').reverse}.png"

  val id = s"$question_family_index-$question_index"

  def executable(implicit db: VirtualTestBase): Executable = new Executable {
    override def execute(): Unit = {
      println(s"Test: $id\n Image: $url \n question: $question\n query: ${prefix + query}\n answer: $answer")
      val res = db.runner.run(prefix + query, Map.empty).records().map(_.get(0).get).toList.headOption
      val should = answer.toString
      println(s"Should be: $should, but get: $res")
      assert(res.contains(answer))
    }
  }
}

class Clevr1000 {
  implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val db: VirtualTestBase = new VirtualTestBase()

  val clevr_1000_small: List[File] = new java.io.File("datasets/CLEVR1000").listFiles().filterNot(_.getName.startsWith(".")).sortBy(_.getName.substring(10,16)).toList

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
  def runnableTest(): Unit = {
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

  @TestFactory
  def testQuestions(): java.util.Collection[DynamicTest] = {

    questions.take(5).map{ question =>DynamicTest.dynamicTest(question.id, question.executable)}
      .asJavaCollection
  }


  @Test
  def threeHop(): Unit = {
    initDB(10)
    db.runner.run(
      """
        |MATCH (i:Image{image_index: 0}) WITH i
        |MATCH (i)~[:contains]~~<objects> WITH i,collect(objects) AS objects
        |MATCH <o:cylinder{color:'green',material:'rubber'}>~[:front]~~<o2{size:'small'}>~[:behind]~~<o3{size:'large',material:'rubber'}>~[:front]~~<o4{size:'large'}>
        |WHERE o IN objects AND o2 IN objects AND o3 IN objects AND o4 IN objects
        |RETURN o,o2,o3,o4
        |""".stripMargin, Map.empty).show()
  }

  @Test
  def oneHop(): Unit = {
    initDB(10)
    db.runner.run(
      """
        |MATCH (i:Image{image_index: 0}) WITH i
        |MATCH (i)~[:contains]~~<objects> WITH i,collect(objects) AS objects
        |MATCH <o:cube>~[r:behind]~~<o2:sphere>
        |WHERE o IN objects AND o2 IN objects
        |return o,r,o2
        |""".stripMargin, Map.empty).show()
  }

  @Test
  def temp(): Unit = {
    db.runner.run(
      """
        |MATCH (a)-[r:b]->(c) RETURN r
        |""".stripMargin, Map.empty)
  }

}