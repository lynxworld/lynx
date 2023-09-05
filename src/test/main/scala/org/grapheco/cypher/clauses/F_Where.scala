package org.grapheco.cypher.clauses

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural._
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-28 17:42
 */
class F_Where extends TestBase{
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("Swedish"), LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Andy"), LynxPropertyKey("age")->LynxValue(36), LynxPropertyKey("belt")->LynxValue("white")))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("Dog")), Map(LynxPropertyKey("name")-> LynxValue("Andy")))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Peter"), LynxPropertyKey("email")-> LynxValue("peter_n@example.com"),LynxPropertyKey("age")-> LynxValue(35)))
  val n4 = TestNode(TestId(4), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name")-> LynxValue("Timothy"), LynxPropertyKey("address")-> LynxValue("Sweden/Malmo"),LynxPropertyKey("age")-> LynxValue(25)))
  val n5 = TestNode(TestId(5), Seq(LynxNodeLabel("Dog")), Map(LynxPropertyKey("name")-> LynxValue("Fido")))
  val n6 = TestNode(TestId(6), Seq(LynxNodeLabel("Dog")), Map(LynxPropertyKey("name")-> LynxValue("Ozzy")))
  val n7 = TestNode(TestId(7), Seq(LynxNodeLabel("Toy")), Map(LynxPropertyKey("name")-> LynxValue("Banana")))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(2), Option(LynxRelationshipType("HAS_DOG")), Map(LynxPropertyKey("since")->LynxValue(2016)))
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(3), Option(LynxRelationshipType("KNOWS")), Map(LynxPropertyKey("since")->LynxValue(1999)))
  val r3 = TestRelationship(TestId(3), TestId(1), TestId(4), Option(LynxRelationshipType("KNOWS")), Map(LynxPropertyKey("since")->LynxValue(2012)))
  val r4 = TestRelationship(TestId(4), TestId(3), TestId(5), Option(LynxRelationshipType("HAS_DOG")), Map(LynxPropertyKey("since")->LynxValue(2010)))
  val r5 = TestRelationship(TestId(5), TestId(3), TestId(6), Option(LynxRelationshipType("HAS_DOG")), Map(LynxPropertyKey("since")->LynxValue(2018)))
  val r6 = TestRelationship(TestId(6), TestId(5), TestId(7), Option(LynxRelationshipType("HAS_TOY")), Map.empty)

  @Before
  def init(): Unit ={
    nodesInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodesInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodesInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))
    nodesInput.append(("n4", NodeInput(n4.labels, n4.props.toSeq)))
    nodesInput.append(("n5", NodeInput(n5.labels, n5.props.toSeq)))
    nodesInput.append(("n6", NodeInput(n6.labels, n6.props.toSeq)))
    nodesInput.append(("n7", NodeInput(n7.labels, n7.props.toSeq)))

    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationsInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), r2.props.toSeq, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))
    relationsInput.append(("r3", RelationshipInput(Seq(r3.relationType.get), r3.props.toSeq, StoredNodeInputRef(r3.startNodeId), StoredNodeInputRef(r3.endNodeId))))
    relationsInput.append(("r4", RelationshipInput(Seq(r4.relationType.get), r4.props.toSeq, StoredNodeInputRef(r4.startNodeId), StoredNodeInputRef(r4.endNodeId))))
    relationsInput.append(("r5", RelationshipInput(Seq(r5.relationType.get), r5.props.toSeq, StoredNodeInputRef(r5.startNodeId), StoredNodeInputRef(r5.endNodeId))))
    relationsInput.append(("r6", RelationshipInput(Seq(r6.relationType.get), r6.props.toSeq, StoredNodeInputRef(r6.startNodeId), StoredNodeInputRef(r6.endNodeId))))

    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
    model.write.commit
  }
  @Test
  def booleanOperator(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name = 'Peter' XOR (n.age < 30 AND n.name = 'Timothy') OR NOT (n.name = 'Timothy' OR n.name = 'Peter')
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(List(LynxValue("Timothy"), LynxValue(25)), List(res(0)("n.name"), res(0)("n.age")))
    Assert.assertEquals(List(LynxValue("Peter"), LynxValue(35)), List(res(1)("n.name"), res(1)("n.age")))
    Assert.assertEquals(List(LynxValue("Andy"), LynxValue(36)), List(res(2)("n.name"), res(2)("n.age")))
  }

  @Test
  def filterOnNodeLabel(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n)
        |WHERE n:Swedish
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("Andy", 36), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def filterOnNodeProperty(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.age < 30
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("Timothy", 25), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def filterOnNodePropertyLess(): Unit = {
    val res = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.age > 99
        |AND  n.age < 37
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("Timothy", 25), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def filterInNodeProperty(): Unit = {
    val res = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name IN ["Timothy"]
        |RETURN n
        |""".stripMargin).records().toArray

    //Assert.assertEquals(List("Timothy", 25), List(res(0)("n.name"), res(0)("n.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def filterOnRelationshipProperty(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (n:Person)-[k:KNOWS]->(f)
        |WHERE k.since < 2000
        |RETURN f.name, f.age, f.email
        |""".stripMargin).records().toArray

    Assert.assertEquals(List("Peter", 35, "peter_n@example.com"), List(res(0)("f.name"), res(0)("f.age"), res(0)("f.email")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def filterOnDynamicallyComputedNodeProperty(): Unit ={
    val res = runOnDemoGraph(
      """
        |WITH 'AGE' AS propname
        |MATCH (n:Person)
        |WHERE n[toLower(propname)] < 30
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(LynxValue("Timothy"), res.head("n.name"))
    Assert.assertEquals(LynxValue(25), res.head("n.age"))
  }

  @Test
  def propertyExistenceChecking(): Unit ={
    val res = runOnDemoGraph(
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
    val res = runOnDemoGraph(
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
    val res = runOnDemoGraph(
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
    val res = runOnDemoGraph(
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
    val res = runOnDemoGraph(
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
    val res = runOnDemoGraph(
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
    val res = runOnDemoGraph(
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
    val res = runOnDemoGraph(
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
    val res = runOnDemoGraph(
      """
        |MATCH
        |  (timothy:Person {name: 'Timothy'}),
        |  (other:Person)
        |WHERE other.name IN ['Andy', 'Peter'] AND (other)-->(timothy)
        |RETURN other.name, other.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Andy", 36), List(res(0)("other.name"), res(0)("other.age")).map(f => f.asInstanceOf[LynxValue].value))
  }

  @Test
  def filterOnPatternsUsingNOT(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH
        |  (person:Person),
        |  (peter:Person {name: 'Peter'})
        |WHERE NOT (person)-->(peter)
        |RETURN person.name, person.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(2, res.length)
    Assert.assertEquals(List(LynxValue("Timothy"), LynxValue(25)), List(res(0)("person.name"), res(0)("person.age")))
    Assert.assertEquals(List(LynxValue("Peter"), LynxValue(35)), List(res(1)("person.name"), res(1)("person.age")))
  }

  @Test
  def filterOnPatternsWithProperties(): Unit ={
    val res = runOnDemoGraph(
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
    val res = runOnDemoGraph(
      """
        |MATCH (n:Person)-[r]->()
        |WHERE n.name='Andy' AND type(r) =~ 'K.*'
        |RETURN type(r), r.since
        |""".stripMargin).records().toArray
    Assert.assertEquals(2, res.length)
    Assert.assertEquals(List(LynxValue("KNOWS"), LynxValue(2012)), List(res(0)("type(r)"), res(0)("r.since")))
    Assert.assertEquals(List(LynxValue("KNOWS"), LynxValue(1999)), List(res(1)("type(r)"), res(1)("r.since")))
  }

  @Test
  def iNOperator(): Unit = {
    val res = runOnDemoGraph(
      """
        |MATCH (a)
        |WHERE a.name IN ['Peter', 'Timothy']
        |RETURN a.name, a.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(2, res.length)
    Assert.assertEquals(List("Timothy", 25), List(res(0)("a.name"), res(0)("a.age")).map(f => f.asInstanceOf[LynxValue].value))
    Assert.assertEquals(List("Peter", 35), List(res(1)("a.name"), res(1)("a.age")).map(f => f.asInstanceOf[LynxValue].value))
  }
//  // manual 6.2
//  @Test
//  def usingExistentialSubqueriesInWHERE(): Unit ={
//    val res = runOnDemoGraph(
//      """
//        |MATCH (person:Person)
//        |WHERE EXISTS {
//        |  MATCH (person)-[:HAS_DOG]->(:Dog)
//        |}
//        |RETURN person.name AS name
//        |""".stripMargin).records().toArray
//    Assert.assertEquals(2, res.length)
//    Assert.assertEquals(List("Andy"), List(res(0)("name")).map(f => f.asInstanceOf[LynxValue].value))
//    Assert.assertEquals(List("Peter"), List(res(1)("name")).map(f => f.asInstanceOf[LynxValue].value))
//  }
//
//  // manual 6.3
//  @Test
//  def nestingExistentialSubqueries(): Unit ={
//    val res = runOnDemoGraph(
//      """
//        |MATCH (person:Person)
//        |WHERE EXISTS {
//        |  MATCH (person)-[:HAS_DOG]->(dog:Dog)
//        |  WHERE EXISTS {
//        |    MATCH (dog)-[:HAS_TOY]->(toy:Toy)
//        |    WHERE toy.name = 'Banana'
//        |  }
//        |}
//        |RETURN person.name AS name
//        |""".stripMargin).records().toArray
//    Assert.assertEquals(1, res.length)
//    Assert.assertEquals(List("Peter"), List(res(0)("name")).map(f => f.asInstanceOf[LynxValue].value))
//  }

  @Test
  def inOperator(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (a:Person)
        |WHERE a.name IN ['Peter', 'Timothy']
        |RETURN a.name, a.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(2, res.length)
    Assert.assertEquals(List(LynxValue("Timothy"), LynxValue(25)), List(res(0)("a.name"), res(0)("a.age")))
    Assert.assertEquals(List(LynxValue("Peter"), LynxValue(35)), List(res(1)("a.name"), res(1)("a.age")))
  }

  @Test
  def defaultToFalseIfPropertyIsMissing(): Unit ={
    val res = runOnDemoGraph(
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
    val res = runOnDemoGraph(
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
    val res = runOnDemoGraph(
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
    val res = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |WHERE n.name >= 'Peter'
        |RETURN n.name, n.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(2, res.length)
    Assert.assertEquals(List(LynxValue("Timothy"), LynxValue(25)), List(res(0)("n.name"), res(0)("n.age")))
    Assert.assertEquals(List(LynxValue("Peter"), LynxValue(35)), List(res(1)("n.name"), res(1)("n.age")))
  }

  @Test
  def compositeRange(): Unit ={
    val res = runOnDemoGraph(
      """
        |MATCH (a:Person)
        |WHERE a.name > 'Andy' AND a.name < 'Timothy'
        |RETURN a.name, a.age
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List("Peter", 35), List(res(0)("a.name"), res(0)("a.age")).map(f => f.asInstanceOf[LynxValue].value))
  }
}
