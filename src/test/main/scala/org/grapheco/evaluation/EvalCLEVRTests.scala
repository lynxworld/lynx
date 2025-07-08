package org.grapheco.evaluation

import org.junit.jupiter.api.Test
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase}

import java.io.File

@Test
class EvalCLEVRTests {

  implicit val neo4j: Driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123"))
  implicit val db: VirtualTestBase = new VirtualTestBase()
  @Test
  def testEvalCLEVR0(): Unit = {
    EvalCLEVR("Find images containing more than 3 objects and less than 10 objects.")
      .query(
        """
          |MATCH (image:Image)~[:contains]~~<object>
          |with image, count(object) as c
          |where c > 3 and c < 10
          |return image.image_index
          |""".stripMargin)
      .save(new File("results/evalCLEVR-0.csv"))
      .eval(
        """
          |MATCH (image:Image)-[:CONTAINS]->(object:Object)
          |with image, count(object) as c
          |where c > 3 and c < 10
          |return image.image_index
          |""".stripMargin)
  }

  @Test
  def testEvalCLEVR1(): Unit = {
    EvalCLEVR("Find images containing a blue object.")
      .query(
        """
          |MATCH (image:Image)~[:contains]~~<object>
          |where object.color = 'blue'
          |return image.image_index
          |""".stripMargin)
      .save(new File("results/evalCLEVR-1.csv"))
      .eval(
        """
          |MATCH (image:Image)-[:CONTAINS]->(object:Object)
          |WHERE object.color = 'blue'
          |RETURN image.image_index
          |""".stripMargin)
  }

  @Test
  def testEvalCLEVR3(): Unit = {
    EvalCLEVR("Find images containing a red cylinder.")
      .query(
        """
          |MATCH (image:Image)~[:contains]~~<object:cylinder>
          |WHERE object.color = 'red'
          |RETURN image.image_index
          |""".stripMargin)
      .save(new File("results/evalCLEVR-3.csv"))
      .eval(
        """
          |MATCH (image:Image)-[:CONTAINS]->(object:Object)
          |WHERE object.color = 'red' AND object.shape = 'cylinder'
          |RETURN image.image_index
          |""".stripMargin)
  }

  @Test
  def testEvalCLEVR4(): Unit = {
    EvalCLEVR("Find images containing a red rubber sphere.")
      .query(
       """
         |MATCH (image:Image)~[:contains]~~<object:sphere>
         |WHERE object.color = 'red' AND object.material = 'rubber'
         |RETURN image.image_index
         |""".stripMargin)
      .save(new File("results/evalCLEVR-4.csv"))
      .eval(
        """
          |MATCH (image:Image)-[:CONTAINS]->(object:Object)
          |WHERE object.color = 'red' AND object.material = 'rubber' AND object.shape = 'sphere'
          |RETURN image.image_index
          |""".stripMargin)
  }
}