package org.grapheco.evaluation

import org.junit.jupiter.api.{DynamicTest, Test, TestFactory}

import scala.collection.JavaConverters.asJavaCollectionConverter

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
