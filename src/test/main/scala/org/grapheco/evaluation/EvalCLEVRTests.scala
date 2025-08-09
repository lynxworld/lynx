package org.grapheco.evaluation

import org.junit.jupiter.api.Test
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase}

import java.io.File

@Test
class EvalCLEVRTests {

  implicit val neo4j: Driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123"))
  implicit val db: VirtualTestBase = new VirtualTestBase()
  @Test
  def testEvalCLEVR1(): Unit = { //540872ms
    EvalCLEVR("Find images containing more than 3 objects and less than 10 objects.")
      .query(
        """
          |MATCH (image:Image)~[:contains]~~<object>
          |with image, count(object) as c
          |where c > 3 and c < 10
          |return image.image_index
          |""".stripMargin)
      .save(new File("results/evalCLEVR-1.csv"))
      .eval(
        """
          |MATCH (image:Image)-[:CONTAINS]->(object:Object)
          |with image, count(object) as c
          |where c > 3 and c < 10
          |return image.image_index
          |""".stripMargin)
      .save(new File("results/evalCLEVR-1-eval.csv")).show
  }

  @Test
  def testEvalCLEVR2(): Unit = { // 866349ms
    EvalCLEVR("Find images contains more than 3 cubes.")
     .query(
        """
          |MATCH (image:Image)~[:contains]~~<object:cube>
          |with image, count(object) as c
          |where c > 3
          |return image.image_index
          |""".stripMargin)
      .save(new File("results/evalCLEVR-2.csv"))
      .eval(
        """
          |MATCH (image:Image)-[:CONTAINS]->(object:Object)
          |WHERE object.shape = 'cube'
          |with image, count(object) as c
          |where c > 3
          |return image.image_index
          |""".stripMargin)
      .save(new File("results/evalCLEVR-2-eval.csv")).show
  }




  @Test
  def testEvalCLEVR3(): Unit = {
    EvalCLEVR("Find images contains a red cylinder.")
      .query(
        """
          |MATCH (image:Image)~[:contains]~~<:cylinder{color:'red'}>
          |return image.image_index
          |""".stripMargin)
      .save(new File("results/evalCLEVR-3.csv"))
      .eval(
        """
          |MATCH (image:Image)-[:CONTAINS]->(object:Object)
          |WHERE object.shape = 'cylinder' and object.color = 'red'
          |return image.image_index
          |""".stripMargin)
     .save(new File("results/evalCLEVR-3-eval.csv")).show
  }


  @Test
  def testEvalCLEVR4(): Unit = { // 1046555ms
    EvalCLEVR("Find images contains more than 2 blue cubes.")
     .query(
        """
          |MATCH (image:Image)~[:contains]~~<object:cube{color:'blue'}>
          |with image, count(object) as c
          |where c > 2
          |return image.image_index
          |""".stripMargin)
      .save(new File("results/evalCLEVR-4.csv"))
      .eval(
        """
          |MATCH (image:Image)-[:CONTAINS]->(object:Object)
          |WHERE object.shape = 'cube' and object.color ='blue'
          |with image, count(object) as c
          |where c > 2
          |return image.image_index
          |""".stripMargin)
      .save(new File("results/evalCLEVR-4-eval.csv")).show
  }
}