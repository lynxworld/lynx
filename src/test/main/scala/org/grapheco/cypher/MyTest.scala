package org.grapheco.cypher

import org.grapheco.lynx.TestBase
import org.junit.{Before, BeforeClass, Test}

class MyTest extends TestBase{
  /*
  (:Person{name: HC, age: 24})-[studyAt]->(:Organization{name: CNIC})
  (:Person{name: ZZH, age: 26})-[studyAt]->(:Organization{name: CNIC})
  (:Person{name: BlueJoe})-[workAt]->(:Organization{name: CNIC})
  (:Person{name: HC, age: 24})-[stuOf]->(:Person{name: BlueJoe})
  (:Person{name: ZZH, age: 26})-[stuOf]->(:Person{name: BlueJoe})
   */
  @Before
  def init(): Unit = {
    this.runOnDemoGraph(
      """
        |Create (h:Person{name: 'HC', age: 24}),
        |(z:Person{name: 'ZZH', age: 26}),
        |(b:Person{name: 'BlueJoe'}),
        |(o:Organization{name: 'CNIC'}),
        |(h)-[:studyAt]->(o),
        |(z)-[:studyAt]->(o),
        |(b)-[:workAt]->(o),
        |(h)-[:stuOf]->(b),
        |(z)-[:stuOf]->(b)
        |""".stripMargin)
  }

  @Test
  def test(): Unit ={
    runOnDemoGraph(
      """
        |match (n{name:'HC'}) return n
        |""".stripMargin).show()
  }

  @Test
  def multiHop(): Unit ={
    runOnDemoGraph(
      """
        | match p = (n{name:'HC'})--()--(r)
        | return r
        |""".stripMargin)
  }
}
