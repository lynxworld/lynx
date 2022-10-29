package org.grapheco.cypher.functions

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural._
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: Wangkainan
 * @create: 2022-08-31 14:01
 */

/*
create ({name:'Alice',eyes:'brown',age:38})
create ({name:'Charlie',eyes:'green',age:53})
create ({name:'Bob',eyes:'blue',age:25})
create ({name:'Daniel',eyes:'brown',age:54})
create ({name:'Eskil',eyes:'blue',age:41,array:['one','two','three']})

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

class D_List extends TestBase {
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("Person"), LynxNodeLabel("Developer")),
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

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(2), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r3 = TestRelationship(TestId(3), TestId(2), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r4 = TestRelationship(TestId(4), TestId(3), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r5 = TestRelationship(TestId(5), TestId(3), TestId(5), Option(LynxRelationshipType("MARRIED")), Map.empty)

  @Before
  def init(): Unit = {
    all_nodes.clear()
    all_rels.clear()

    nodesInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodesInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodesInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))
    nodesInput.append(("n4", NodeInput(n4.labels, n4.props.toSeq)))
    nodesInput.append(("n5", NodeInput(n5.labels, n5.props.toSeq)))

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
  }

  /*
       deprecated in cypher 3.5
      */
  @Test
  def extract(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH p =(a)-->(b)-->(c)
        |WHERE a.name = 'Alice' AND b.name = 'Bob' AND c.name = 'Daniel'
        |RETURN extract(n IN nodes(p)| n.age) AS extracted
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(LynxList(List(LynxValue(38), LynxValue(25), LynxValue(54))), records(0)("extracted"))
  }

  /*
       deprecated in cypher 3.5
      */
  @Test
  def filter(): Unit = {

    val records = runOnDemoGraph(
      """
        |MATCH (a)
        |WHERE a.name = 'Eskil'
        |RETURN a.array, filter(x IN a.array WHERE size(x)= 3)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(List(LynxString("one"), LynxString("two"), LynxString("three")), records(0)("a.array").asInstanceOf[LynxValue].value)
    Assert.assertEquals(List(LynxString("one"), LynxString("two")), records(0)("filter(x IN a.array WHERE size(x)= 3)").asInstanceOf[LynxValue].value)
  }

  @Test
  def keys(): Unit = {

    val records = runOnDemoGraph(
      """
        |MATCH (a)
        |WHERE a.name = 'Alice'
        |RETURN keys(a)
        |""".stripMargin).records().toArray

    val array_Expect = List(LynxString("name"), LynxString("eyes"), LynxString("age"))
    val array_Actual = records(0)("keys(a)").asInstanceOf[LynxList].value
    Assert.assertEquals(1, records.length)
    Assert.assertEquals(array_Expect.diff(array_Actual), array_Actual.diff(array_Expect))
  }

  @Test
  def labels(): Unit = {

    val records = runOnDemoGraph(
      """
        |MATCH (a)
        |WHERE a.name = 'Alice'
        |RETURN labels(a)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(List(LynxString("Person"), LynxString("Developer")), records(0)("labels(a)").asInstanceOf[LynxValue].value)
  }

  @Test
  def nodes(): Unit = {

    val records = runOnDemoGraph(
      """
        |MATCH p =(a)-->(b)-->(c)
        |WHERE a.name = 'Alice' AND c.name = 'Eskil'
        |RETURN nodes(p)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(List(n1, n3, n5), records(0)("nodes(p)").asInstanceOf[LynxList].value)
  }

  @Test
  def range(): Unit = {

    val records = runOnDemoGraph(
      """
        |RETURN range(0, 10), range(2, 18, 3)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(List(LynxInteger(0), LynxInteger(1), LynxInteger(2), LynxInteger(3), LynxInteger(4), LynxInteger(5),
      LynxInteger(6), LynxInteger(7), LynxInteger(8), LynxInteger(9), LynxInteger(10)), records(0)("range(0, 10)").asInstanceOf[LynxValue].value)

    Assert.assertEquals(List(LynxInteger(2), LynxInteger(5), LynxInteger(8), LynxInteger(11), LynxInteger(14), LynxInteger(17)),
      records(0)("range(2, 18, 3)").asInstanceOf[LynxValue].value)
  }

  @Test
  def reduce(): Unit = {

    val records = runOnDemoGraph(
      """
        |MATCH p =(a)-->(b)-->(c)
        |WHERE a.name = 'Alice' AND b.name = 'Bob' AND c.name = 'Daniel'
        |RETURN reduce(totalAge = 0, n IN nodes(p)| totalAge + n.age) AS reduction
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(LynxValue(117), records(0)("reduction"))
  }

  @Test
  def relationships(): Unit = {

    val records = runOnDemoGraph(
      """
        |MATCH p =(a)-->(b)-->(c)
        |WHERE a.name = 'Alice' AND c.name = 'Eskil'
        |RETURN relationships(p)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(List(r2,r5), records(0)("relationships(p)").asInstanceOf[LynxValue].value)
  }

  @Test
  def reverse(): Unit = {

    val records = runOnDemoGraph(
      """
        |WITH [4923,'abc',521, NULL , 487] AS ids
        |RETURN reverse(ids)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(LynxList(List(LynxValue(487),LynxValue(null),LynxValue(521),LynxValue("abc"),LynxValue(4923))), records(0)("reverse(ids)"))
  }

  @Test
  def tail(): Unit = {

    val records = runOnDemoGraph(
      """
        |MATCH (a)
        |WHERE a.name = 'Eskil'
        |RETURN a.array, tail(a.array)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(List(LynxString("one"), LynxString("two"), LynxString("three")), records(0)("a.array").asInstanceOf[LynxValue].value)
    Assert.assertEquals(List(LynxString("two"), LynxString("three")), records(0)("tail(a.array)").asInstanceOf[LynxValue].value)
  }
}

