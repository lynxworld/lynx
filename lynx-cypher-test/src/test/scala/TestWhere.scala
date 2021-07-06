import org.grapheco.lynx.{LynxInteger, LynxString, LynxValue}
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-07-06 10:20
 */
class TestWhere {
  val n1 = TestNode(1, Seq("Swedish", "Person"), ("name",LynxString("Andy")), ("age", LynxInteger(36)), ("belt", LynxString("white")))
  val n2 = TestNode(2, Seq("Dog"), ("name",LynxString("Andy")))
  val n3 = TestNode(3, Seq("Person"), ("name",LynxString("Peter")), ("email", LynxString("peter_n@example.com")), ("age", LynxInteger(35)))
  val n4 = TestNode(4, Seq("Person"), ("name",LynxString("Timothy")), ("address", LynxString("Sweden/Malmo")),("age", LynxInteger(25)))
  val n5 = TestNode(5, Seq("Dog"), ("name",LynxString("Fido")))
  val n6 = TestNode(6, Seq("Dog"), ("name",LynxString("Ozzy")))
  val n7 = TestNode(7, Seq("Toy"), ("name",LynxString("Banana")))

  val r1 = TestRelationship(1, 1, 2, Option("HAS_DOG"), ("since", LynxInteger(2016)))
  val r2 = TestRelationship(2, 1, 3, Option("KNOWS"), ("since", LynxInteger(1999)))
  val r3 = TestRelationship(3, 1, 4, Option("KNOWS"), ("since", LynxInteger(2012)))
  val r4 = TestRelationship(4, 3, 5, Option("HAS_DOG"), ("since", LynxInteger(2010)))
  val r5 = TestRelationship(5, 3, 6, Option("HAS_DOG"), ("since", LynxInteger(2018)))
  val r6 = TestRelationship(6, 5, 7, Option("HAS_TOY"))


  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4, n5, n6, n7)
  relsBuffer.append(r1, r2, r3, r4, r5, r6)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def booleanOperator(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name = 'Peter' XOR (n.age < 30 AND n.name = 'Timothy') OR NOT (n.name = 'Timothy' OR n.name = 'Peter')
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("Andy", 36), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("Peter", 35), List(res(1)("n.name"), res(1)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("Timothy", 25), List(res(2)("n.name"), res(2)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def filterOnNodeLabel(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n)
        |WHERE n:Swedish
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("Andy", 36), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
   }

  @Test
  def filterOnNodeProperty(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.age < 30
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("Timothy", 25), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def filterOnRelationshipProperty(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)-[k:KNOWS]->(f)
        |WHERE k.since < 2000
        |RETURN f.name, f.age, f.email
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("Peter", 35, "peter_n@example.com"), List(res(0)("f.name"), res(0)("f.age"), res(0)("f.email")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def filterOnDynamicallyComputedNodeProperty(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |WITH 'AGE' AS propname
        |MATCH (n:Person)
        |WHERE n[toLower(propname)] < 30
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("Peter", 35, "peter_n@example.com"), List(res(0)("f.name"), res(0)("f.age"), res(0)("f.email")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def propertyExistenceChecking(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.belt IS NOT NULL
        |RETURN n.name, n.belt
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Andy", "white"), List(res(0)("n.name"), res(0)("n.belt")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def prefixStringSearchUsingSTARTSWITH(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name STARTS WITH 'Pet'
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Peter", 35), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def suffixStringSearchUsingENDSWITH(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name ENDS WITH 'ter'
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Peter", 35), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def substringSearchUsingCONTAINS(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name CONTAINS 'ete'
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Peter", 35), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def stringMatchingNegation(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE NOT n.name ENDS WITH 'y'
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Peter", 35), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def matchingUsingRegularExpressions(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name =~ 'Tim.*'
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Timothy", 25), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def escapingInRegularExpressions(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.email =~ '.*\\.com'
        |RETURN n.name, n.age, n.email
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Peter", 35, "peter_n@example.com"), List(res(0)("n.name"), res(0)("n.age"), res(0)("n.email")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def caseInsensitiveRegularExpressions(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name =~ '(?i)AND.*'
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Andy", 36), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def filterOnPatterns(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH
        |  (timothy:Person {name: 'Timothy'}),
        |  (other:Person)
        |WHERE other.name IN ['Andy', 'Peter'] AND (other)-->(timothy)
        |RETURN other.name, other.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Andy", 36), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def filterOnPatternsUsingNOT(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH
        |  (person:Person),
        |  (peter:Person {name: 'Peter'})
        |WHERE NOT (person)-->(peter)
        |RETURN person.name, person.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(2, res.length)
    Assert.assertEquals(List("Peter", 35), List(res(0)("person.name"), res(0)("person.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("Timothy", 25), List(res(1)("person.name"), res(1)("person.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def filterOnPatternsWithProperties(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE (n)-[:KNOWS]-({name: 'Timothy'})
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Andy", 36), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def filterOnRelationshipType(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)-[r]->()
        |WHERE n.name='Andy' AND type(r) =~ 'K.*'
        |RETURN type(r), r.since
        |""".stripMargin).records().toArray
    Assert.assertEquals(2, res.length)
    Assert.assertEquals(List("KNOWS", 1999), List(res(0)("type(r)"), res(0)("r.since")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("KNOWS", 2012), List(res(1)("type(r)"), res(1)("r.since")).map(f => f.asInstanceOf[LynxValue].value))
  }

  // manual 6.2
  @Test
  def usingExistentialSubqueriesInWHERE(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(:Dog)
        |}
        |RETURN person.name AS name
        |""".stripMargin).records().toArray
    Assert.assertEquals(2, res.length)
    Assert.assertEquals(List("Andy"), List(res(0)("name")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("Peter"), List(res(1)("name")).map(f => f.asInstanceOf[LynxValue].value))
  }

  // manual 6.3
  @Test
  def nestingExistentialSubqueries(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (person:Person)
        |WHERE EXISTS {
        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
        |  WHERE EXISTS {
        |    MATCH (dog)-[:HAS_TOY]->(toy:Toy)
        |    WHERE toy.name = 'Banana'
        |  }
        |}
        |RETURN person.name AS name
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Peter"), List(res(0)("name")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def inOperator(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (a:Person)
        |WHERE a.name IN ['Peter', 'Timothy']
        |RETURN a.name, a.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(2, res.length)
    Assert.assertEquals(List("Peter", 35), List(res(0)("a.name"), res(0)("a.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("Timothy", 25), List(res(1)("a.name"), res(1)("a.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def defaultToFalseIfPropertyIsMissing(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.belt = 'white'
        |RETURN n.name, n.age, n.belt
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Andy", 36, "white"), List(res(0)("n.name"), res(0)("n.age"), res(0)("n.belt")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def defaultToTrueIfPropertyIsMissing(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.belt = 'white' OR n.belt IS NULL
        |RETURN n.name, n.age, n.belt
        |ORDER BY n.name
        |""".stripMargin).records().toArray
    Assert.assertEquals(3, res.length)
    Assert.assertEquals(List("Andy", 36, "white"), List(res(0)("n.name"), res(0)("n.age"), res(0)("n.belt")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("Peter", 35, null), List(res(1)("n.name"), res(1)("n.age"), res(1)("n.belt")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("Timothy", 25, null), List(res(2)("n.name"), res(2)("n.age"), res(2)("n.belt")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def filterOnNull(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (person:Person)
        |WHERE person.name = 'Peter' AND person.belt IS NULL
        |RETURN person.name, person.age, person.belt
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Peter", 35, null), List(res(0)("person.name"), res(0)("person.age"), res(0)("person.belt")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def simpleRange(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name >= 'Peter'
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(2, res.length)
    Assert.assertEquals(List("Peter", 35), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("Timothy", 25), List(res(1)("n.name"), res(1)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def compositeRange(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |MATCH (a:Person)
        |WHERE a.name > 'Andy' AND a.name < 'Timothy'
        |RETURN a.name, a.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Peter", 35), List(res(0)("a.name"), res(0)("a.age")).map(f => f.asInstanceOf[LynxValue].value))
  }
}
