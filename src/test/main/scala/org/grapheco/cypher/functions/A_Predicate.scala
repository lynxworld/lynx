package org.grapheco.cypher.functions

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.runner.{NodeFilter, RelationshipFilter}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxNull, LynxString}
import org.grapheco.lynx.types.structural._
import org.junit.{Assert, Before, Test}
import org.opencypher.v9_0.expressions.SemanticDirection

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: Wangkainan
 * @create: 2022-08-29 14:20
 */


/*

create ({name:'Alice',eyes:'brown',age:38})
create ({name:'Charlie',eyes:'green',age:53})
create ({name:'Bob',eyes:'blue',age:25})
create ({name:'Daniel',eyes:'brown',age:54})
create ({name:'Eskil',eyes:'blue',age:41,array:['one','two','three']})
create ({eyes:'brown',age:61})

match (a),(b)
where a.name='Alice' and b.name='Bob'
create (a)-[r:KNOWS]->(b)

match (a),(c)
where a.name='Alice' and c.name='Charlie'
create (a)-[r:KNOWS]->(c)

match (b),(d)
where b.name='Bob' and d.name='Daniel'
create (b)-[r:KNOWS]->(d)

match (d),(c)
where c.name='Charlie' and d.name='Daniel'
create (c)-[r:KNOWS]->(d)

match (b),(e)
where b.name='Bob' and e.name='Eskil'
create (b)-[r:MARRIED]->(e)


  */

class A_Predicate extends TestBase {
//  val nodeCreateCyphers = Array(
//    "create ({name:'Alice',eyes:'brown',age:38})",
//    "create ({name:'Charlie',eyes:'green',age:53})",
//    "create ({name:'Bob',eyes:'blue',age:25})",
//    "create ({name:'Daniel',eyes:'brown',age:54})",
//    "create ({name:'Eskil',eyes:'blue',age:41,array:['one','two','three']})",
//    "create ({eyes:'brown',age:61})"
//  )
//
//  val relCreateCyphers = Array(
//    """
//      |match (a),(b)
//      |where a.name='Alice' and b.name='Bob'
//      |create (a)-[r:KNOWS]->(b)
//      |""".stripMargin,
//    """
//      |match (a),(c)
//      |where a.name='Alice' and c.name='Charlie'
//      |create (a)-[r:KNOWS]->(c)
//      |""".stripMargin,
//    """
//      |match (b),(d)
//      |where b.name='Bob' and d.name='Daniel'
//      |create (b)-[r:KNOWS]->(d)
//      |""".stripMargin,
//    """
//    |match (d),(c)
//    |where c.name='Charlie' and d.name='Daniel'
//    |create (c)-[r:KNOWS]->(d)
//    |""".stripMargin,
//
//    """
//      |match (b),(e)
//      |where b.name='Bob' and e.name='Eskil'
//      |create (b)-[r:MARRIED]->(e)
//      |""".stripMargin
//  )
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq.empty,
    Map(LynxPropertyKey("name") -> LynxValue("Alice"),
      LynxPropertyKey("age") -> LynxValue(38),
      LynxPropertyKey("eyes") -> LynxValue("brown")))
  val n2 = TestNode(TestId(2), Seq.empty,
    Map(LynxPropertyKey("name") -> LynxValue("Charlie"),
      LynxPropertyKey("age") -> LynxValue(53),
      LynxPropertyKey("eyes") -> LynxValue("green")))
  val n3 = TestNode(TestId(3), Seq.empty,
    Map(LynxPropertyKey("name") -> LynxValue("Bob"),
      LynxPropertyKey("age") -> LynxValue(25),
      LynxPropertyKey("eyes") -> LynxValue("blue")))
  val n4 = TestNode(TestId(4), Seq.empty,
    Map(LynxPropertyKey("name") -> LynxValue("Daniel"),
      LynxPropertyKey("age") -> LynxValue(54),
      LynxPropertyKey("eyes") -> LynxValue("brown")))
  val n5 = TestNode(TestId(5), Seq.empty,
    Map(LynxPropertyKey("name") -> LynxValue("Eskil"),
      LynxPropertyKey("age") -> LynxValue(41),
      LynxPropertyKey("eyes") -> LynxValue("brown"),
      LynxPropertyKey("array") -> LynxValue(Array("one", "two", "three"))))
  val n6 = TestNode(TestId(6), Seq.empty,
    Map(LynxPropertyKey("age") -> LynxValue(61),
      LynxPropertyKey("eyes") -> LynxValue("brown")))
  val r1 = TestRelationship(TestId(1), TestId(1), TestId(2), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r3 = TestRelationship(TestId(3), TestId(2), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r4 = TestRelationship(TestId(4), TestId(3), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r5 = TestRelationship(TestId(5), TestId(3), TestId(5), Option(LynxRelationshipType("MARRIED")), Map.empty)

  @Before
  def init(): Unit = {
//    nodeCreateCyphers.foreach(cypher=>runOnDemoGraph(cypher))
//    relCreateCyphers.foreach(cypher=>runOnDemoGraph(cypher))
    this.all_nodes.clear()
    this.all_rels.clear()
    nodesInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodesInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodesInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))
    nodesInput.append(("n4", NodeInput(n4.labels, n4.props.toSeq)))
    nodesInput.append(("n5", NodeInput(n5.labels, n5.props.toSeq)))
    nodesInput.append(("n6", NodeInput(n6.labels, n6.props.toSeq)))

    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationsInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), Seq.empty, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))
    relationsInput.append(("r3", RelationshipInput(Seq(r3.relationType.get), Seq.empty, StoredNodeInputRef(r3.startNodeId), StoredNodeInputRef(r3.endNodeId))))
    relationsInput.append(("r4", RelationshipInput(Seq(r4.relationType.get), Seq.empty, StoredNodeInputRef(r4.startNodeId), StoredNodeInputRef(r4.endNodeId))))
    relationsInput.append(("r5", RelationshipInput(Seq(r5.relationType.get), Seq.empty, StoredNodeInputRef(r5.startNodeId), StoredNodeInputRef(r5.endNodeId))))

    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
    model.write.commit
  }

  @Test
  def all(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH  p =(a)-[*1..3]->(b)
        |WHERE a.name = 'Alice' AND b.name = 'Daniel' AND ALL (x IN nodes(p) WHERE x.age > 30)
        |RETURN p
        |""".stripMargin).records().map(f => f("p").asInstanceOf[LynxValue].value).toArray
    Assert.assertEquals(1, records.length)
  }

  @Test
  def any(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (a)
        |WHERE a.name = 'Eskil' AND ANY (x IN a.array WHERE x = 'one')
        |RETURN a.name, a.array
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("Eskil", records(0)("a.name").asInstanceOf[LynxValue].value)
    Assert.assertEquals(List(LynxString("one"), LynxString("two"), LynxString("three")), records.head("a.array").asInstanceOf[LynxValue].value)
  }

  @Test
  def exists_1(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH n
        |WHERE exists(n.name)
        |RETURN n.name AS name, exists((n)-[:MARRIED]->()) AS is_married
        |""".stripMargin).records().toArray

    Assert.assertEquals(5, records.length)
    for (record <- records) {
      record("name").asInstanceOf[LynxValue].value match {
        case "Alice" => Assert.assertEquals(false, record("is_married").asInstanceOf[LynxValue].value)
        case "Bob" => Assert.assertEquals(true, record("is_married").asInstanceOf[LynxValue].value)
        case "Charlie" => Assert.assertEquals(false, record("is_married").asInstanceOf[LynxValue].value)
        case "Daniel" => Assert.assertEquals(false, record("is_married").asInstanceOf[LynxValue].value)
        case "Eskil" => Assert.assertEquals(false, record("is_married").asInstanceOf[LynxValue].value)
        case _ => Assert.assertEquals(true, false)
      }
    }
  }

  @Test
  def exists_2(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (a),(b)
        |WHERE exists(a.name) AND NOT exists(b.name)
        |OPTIONAL MATCH (c:DoesNotExist)
        |RETURN a.name AS a_name, b.name AS b_name, exists(b.name) AS b_has_name, c.name AS c_name, exists(c.name) AS c_has_name
        |ORDER BY a_name, b_name, c_name
        |Limit 1
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(LynxValue("Alice"), records(0)("a_name"))
    Assert.assertEquals(LynxValue(null), records(0)("b_name"))
    Assert.assertEquals(LynxValue(false), records(0)("b_has_name"))
    Assert.assertEquals(LynxNull, records(0)("c_name"))
    Assert.assertEquals(LynxValue(false), records(0)("c_has_name"))
  }

  @Test
  def none(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH p =(n)-[*1..3]->(b)
        |WHERE n.name = 'Alice' AND NONE (x IN nodes(p) WHERE x.age = 25)
        |RETURN p
        |""".stripMargin).records().map(f => f("p").asInstanceOf[LynxValue].value).toArray

    Assert.assertEquals(2, records.length)
    Assert.assertEquals(List(n1, r1, n2 ), records(0))
    Assert.assertEquals(List(n1, r1, n2, r3, n4), records(1))
  }

  @Test
  def single(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH p = (n)-->(b)
        |WHERE
        |  n.name = 'Alice'
        |  AND single(var IN nodes(p) WHERE var.eyes = 'blue')
        |RETURN p
        |""".stripMargin).records().map(f => f("p")).toArray
    Assert.assertEquals(1, records.length)
    Assert.assertEquals(List(n1, LynxList(List(r2, n3, LynxList(List())))), records(0).value)
  }
}

