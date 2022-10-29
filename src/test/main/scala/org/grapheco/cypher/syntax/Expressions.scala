package org.grapheco.cypher.syntax
import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.structural._
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer


/*
create ({name:'Alice',eyes:'brown',age:38})
create ({name:'Charlie',eyes:'green',age:53})
create ({name:'Bob',eyes:'blue',age:25})
create ({name:'Daniel',eyes:'brown'})
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

class Expressions extends TestBase {
  val nodeInput = ArrayBuffer[(String, NodeInput)]()
  val relationshipInput = ArrayBuffer[(String, RelationshipInput)]()

  val A = TestNode(TestId(1), Seq(LynxNodeLabel("A")), Map(LynxPropertyKey("name") -> LynxValue("Alice"), LynxPropertyKey("eyes") -> LynxValue("brown"), LynxPropertyKey("age") -> LynxValue(38)))
  val B = TestNode(TestId(2), Seq(LynxNodeLabel("B")), Map(LynxPropertyKey("name") -> LynxValue("Bob"), LynxPropertyKey("eyes") -> LynxValue("blue"), LynxPropertyKey("age") -> LynxValue(25)))
  val C = TestNode(TestId(3), Seq(LynxNodeLabel("C")), Map(LynxPropertyKey("name") -> LynxValue("Charlie"), LynxPropertyKey("eyes") -> LynxValue("green"), LynxPropertyKey("age") -> LynxValue(53)))
  val D = TestNode(TestId(4), Seq(LynxNodeLabel("D")), Map(LynxPropertyKey("name") -> LynxValue("Daniel"), LynxPropertyKey("eyes") -> LynxValue("brown")))
  val E = TestNode(TestId(5), Seq(LynxNodeLabel("E")), Map(LynxPropertyKey("array") -> LynxList(List(LynxValue("one"), LynxValue("two"), LynxValue("three"))), LynxPropertyKey("name") -> LynxValue("Eskil"), LynxPropertyKey("eyes") -> LynxValue("blue"), LynxPropertyKey("age") -> LynxValue(41)))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(2), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r2 = TestRelationship(TestId(1), TestId(1), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r3 = TestRelationship(TestId(1), TestId(3), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r4 = TestRelationship(TestId(1), TestId(2), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r5 = TestRelationship(TestId(1), TestId(2), TestId(5), Option(LynxRelationshipType("MARRIED")), Map.empty)

  @Before
  def init(): Unit = {
    all_nodes.clear()
    all_rels.clear()
    nodeInput.append(("A", NodeInput(A.labels, A.props.toSeq)))
    nodeInput.append(("B", NodeInput(B.labels, B.props.toSeq)))
    nodeInput.append(("C", NodeInput(C.labels, C.props.toSeq)))
    nodeInput.append(("D", NodeInput(D.labels, D.props.toSeq)))
    nodeInput.append(("E", NodeInput(E.labels, E.props.toSeq)))

    relationshipInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationshipInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), Seq.empty, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))
    relationshipInput.append(("r3", RelationshipInput(Seq(r3.relationType.get), Seq.empty, StoredNodeInputRef(r3.startNodeId), StoredNodeInputRef(r3.endNodeId))))
    relationshipInput.append(("r4", RelationshipInput(Seq(r4.relationType.get), Seq.empty, StoredNodeInputRef(r4.startNodeId), StoredNodeInputRef(r4.endNodeId))))
    relationshipInput.append(("r5", RelationshipInput(Seq(r5.relationType.get), Seq.empty, StoredNodeInputRef(r5.startNodeId), StoredNodeInputRef(r5.endNodeId))))

    model.write.createElements(nodeInput, relationshipInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      })
  }

  @Test
  def simpleCASEForm(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN
        |CASE n.eyes
        |WHEN 'blue'
        |THEN 1
        |WHEN 'brown'
        |THEN 2
        |ELSE 3 END AS result
        |""".stripMargin).records().map(f => f("result").value).toArray.sortBy(r => r.asInstanceOf[Long])
    val expectResult = Array(2l, 1l, 3l, 2l, 1l).sorted
    compareArray(expectResult,records)
  }

  @Test
  def genericCASEForm(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN
        |CASE
        |WHEN n.eyes = 'blue'
        |THEN 1
        |WHEN n.age < 40
        |THEN 2
        |ELSE 3 END AS result
        |""".stripMargin).records().map(f => f("result").value).toArray.sortBy(r => r.asInstanceOf[Long])
    val expectResult = Array(2l, 1l, 3l, 3l, 1l).sorted
    compareArray(expectResult,records)
  }

  @Test
  def diffSimBetweenGeneEx1(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name,
        |CASE n.age
        |WHEN n.age IS NULL THEN -1
        |ELSE n.age - 10 END AS age_10_years_ago
        |""".stripMargin).records().map(f => {
      Map("name" -> f("n.name"), "age_10_years_ago" -> f("age_10_years_ago"))
    }).toArray.sortBy(e => e("name").toString)
    val expectResult = Array(
      Map("name" -> "Alice", "age_10_years_ago" -> 28),
      Map("name" -> "Bob", "age_10_years_ago" -> 15),
      Map("name" -> "Charlie", "age_10_years_ago" -> 43),
      Map("name" -> "Daniel", "age_10_years_ago" -> null),
      Map("name" -> "Eskil", "age_10_years_ago" -> 31)
    ).sortBy(e => e("name").toString)

    Assert.assertEquals(expectResult.length, records.length)
    for (i <- 0 to records.length - 1) {
      Assert.assertEquals(LynxValue(expectResult(i)("name")), records(i)("name"))
      Assert.assertEquals(LynxValue(expectResult(i)("age_10_years_ago")), records(i)("age_10_years_ago"))
    }
  }

  @Test
  def diffSimBetweenGeneEx2(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n)
        |RETURN n.name,
        |CASE
        |WHEN n.age IS NULL THEN -1
        |ELSE n.age - 10 END AS age_10_years_ago
        |""".stripMargin).records().map(f => {
      Map("name" -> f("n.name"), "age_10_years_ago" -> f("age_10_years_ago"))
    }).toArray.sortBy(e => e("name").toString)
    val expectResult = Array(
      Map("name" -> "Alice", "age_10_years_ago" -> 28),
      Map("name" -> "Bob", "age_10_years_ago" -> 15),
      Map("name" -> "Charlie", "age_10_years_ago" -> 43),
      Map("name" -> "Daniel", "age_10_years_ago" -> -1),
      Map("name" -> "Eskil", "age_10_years_ago" -> 31)
    ).sortBy(e => e("name").toString)

    Assert.assertEquals(expectResult.length, records.length)
    for (i <- 0 to records.length - 1) {
      Assert.assertEquals(LynxValue(expectResult(i)("name")), records(i)("name"))
      Assert.assertEquals(LynxValue(expectResult(i)("age_10_years_ago")), records(i)("age_10_years_ago"))
    }
  }


  /**
   * compare expect Result with actual Result
   *
   * @param expectResult
   * @param records
   * @tparam A
   */
  def compareArray[A](expectResult: Array[A], records: Array[Any]): Unit = {
    Assert.assertEquals(expectResult.length, records.length)
    for (i <- 0 to records.length - 1) {
      Assert.assertEquals(expectResult(i), records(i))
    }
  }
}

