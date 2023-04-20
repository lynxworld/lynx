package org.grapheco.cypher.syntax

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.structural._
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer


/*
create (a:Person{name:'Keanu Reeves'})

create (m:Movie{title : 'The Devils Advocate',release: 1997})
create (m:Movie{title : 'The Matrix',release: 1999})
create (m:Movie{title : 'The Replacements',release: 2000})
create (m:Movie{title : 'The Matrix Reloaded',release: 2003})
create (m:Movie{title : 'The Matrix Revolutions',release: 2003})
create (m:Movie{title : 'Somethings Gotta Give',release: 2003})
create (m:Movie{title : 'Johnny Mnemonic',release: 1995})

match (a:Person),(m)
where a.name='Keanu Reeves' and m.title='The Devils Advocate'
create (a)-[r:ACTION_IN]->(m)

match (a:Person),(m)
where a.name='Keanu Reeves' and m.title='The Matrix'
create (a)-[r:ACTION_IN]->(m)

match (a:Person),(m)
where a.name='Keanu Reeves' and m.title='The Replacements'
create (a)-[r:ACTION_IN]->(m)

match (a:Person),(m)
where a.name='Keanu Reeves' and m.title='The Matrix Reloaded'
create (a)-[r:ACTION_IN]->(m)

match (a:Person),(m)
where a.name='Keanu Reeves' and m.title='The Matrix Revolutions'
create (a)-[r:ACTION_IN]->(m)

match (a:Person),(m)
where a.name='Keanu Reeves' and m.title='Somethings Gotta Give'
create (a)-[r:ACTION_IN]->(m)

match (a:Person),(m)
where a.name='Keanu Reeves' and m.title='Johnny Mnemonic'
create (a)-[r:ACTION_IN]->(m)

*/

class TestList extends TestBase {

  @Test
  def listsInGeneral_1(): Unit = {
    val records = runOnDemoGraph("RETURN [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] AS list").records().map(f => f("list").asInstanceOf[LynxList].value).toArray
    compareArray(Array.range(0,10).map(f=>LynxValue(f)),records(0).toArray)
  }

  @Test
  def listsInGeneral_2(): Unit = {
    val records = runOnDemoGraph("RETURN range(0, 10)[3]").records().map(f => f("range(0, 10)[3]").asInstanceOf[LynxValue]).toArray
    Assert.assertEquals(LynxValue(3), records(0))
  }

  @Test
  def listsInGeneral_3(): Unit = {
    val records = runOnDemoGraph("RETURN range(0, 10)[-3]").records().map(f => f("range(0, 10)[-3]").asInstanceOf[LynxValue]).toArray
    Assert.assertEquals(LynxValue(8), records(0))
  }

  @Test
  def listsInGeneral_4(): Unit = {
    val records = runOnDemoGraph("RETURN range(0, 10)[0..3]").records().map(f => f("range(0, 10)[0..3]").asInstanceOf[LynxList].value).toArray
    Assert.assertEquals(3, records(0).length)
  }

  @Test
  def listsInGeneral_5(): Unit = {
    val records = runOnDemoGraph("RETURN range(0, 10)[0..-5]").records().map(f => f("range(0, 10)[0..-5]").asInstanceOf[LynxList].value).toArray
    compareArray(Array.range(0,6).map(f=>LynxValue(f)),records(0).toArray)
  }

  @Test
  def listsInGeneral_6(): Unit = {
    val records = runOnDemoGraph("RETURN range(0, 10)[-5..]").records().map(f => f("range(0, 10)[-5..]").asInstanceOf[LynxList].value).toArray
    compareArray(Array.range(6,11).map(f=>LynxValue(f)),records(0).toArray)
  }

  @Test
  def listsInGeneral_7(): Unit = {
    val records = runOnDemoGraph("RETURN range(0, 10)[..4]").records().map(f => f("range(0, 10)[..4]").asInstanceOf[LynxList].value).toArray
    compareArray(Array.range(0,4).map(f=>LynxValue(f)),records(0).toArray)
  }

  @Test
  def listsInGeneral_8(): Unit = {
    val records = runOnDemoGraph("RETURN range(0, 10)[15]").records().map(f => f("range(0, 10)[15]").asInstanceOf[LynxValue].value).toArray
    Assert.assertEquals(null, records(0))
  }

  @Test
  def listsInGeneral_9(): Unit = {
    val records = runOnDemoGraph("RETURN range(0, 10)[5..15]").records().map(f => f("range(0, 10)[5..15]").asInstanceOf[LynxList].value).toArray
    compareArray(Array.range(5,11).map(f=>LynxValue(f)),records(0).toArray)
  }

  @Test
  def listsInGeneral_10(): Unit = {
    val records = runOnDemoGraph("RETURN size(range(0, 10)[0..3])").records().map(f => f("size(range(0, 10)[0..3])").asInstanceOf[LynxValue]).toArray
    Assert.assertEquals(LynxValue(3), records(0))
  }

  @Test
  def listComprehension(): Unit = {
    val records = runOnDemoGraph("RETURN [x IN range(0,10) WHERE x % 2 = 0 | x^3] AS result").records().map(f => f("result").asInstanceOf[LynxList].value).toArray
    val expectResult = (for {i <- 0 to 10 if i % 2 == 0} yield LynxValue(i * i * i)).toArray
    compareArray(expectResult,records(0).toArray)
  }

  @Test
  def patternComprehension(): Unit = {
    val nodesInput = ArrayBuffer[(String, NodeInput)]()
    val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

    val p = TestNode(TestId(0), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name") -> LynxValue("Keanu Reeves")))
    val m1 = TestNode(TestId(1), Seq(LynxNodeLabel("Movie")), Map(
      LynxPropertyKey("title") -> LynxValue("The Devil Advocate"), LynxPropertyKey("released") -> LynxValue(1997)
    ))

    val m2 = TestNode(TestId(2), Seq(LynxNodeLabel("Movie")), Map(
      LynxPropertyKey("title") -> LynxValue("The Matrix"), LynxPropertyKey("released") -> LynxValue(1999)
    ))

    val m3 = TestNode(TestId(3), Seq(LynxNodeLabel("Movie")), Map(
      LynxPropertyKey("title") -> LynxValue("The Replacements"), LynxPropertyKey("released") -> LynxValue(2000)
    ))

    val m4 = TestNode(TestId(4), Seq(LynxNodeLabel("Movie")), Map(
      LynxPropertyKey("title") -> LynxValue("The Matrix Reloaded"), LynxPropertyKey("released") -> LynxValue(2003)
    ))

    val m5 = TestNode(TestId(5), Seq(LynxNodeLabel("Movie")), Map(
      LynxPropertyKey("title") -> LynxValue("The Matrix Revolutions"), LynxPropertyKey("released") -> LynxValue(2003)
    ))

    val m6 = TestNode(TestId(6), Seq(LynxNodeLabel("Movie")), Map(
      LynxPropertyKey("title") -> LynxValue("The Somethings Gotta Give"), LynxPropertyKey("released") -> LynxValue(2003)
    ))

    val m7 = TestNode(TestId(7), Seq(LynxNodeLabel("Movie")), Map(
      LynxPropertyKey("title") -> LynxValue("The Johnny Mnemonic"), LynxPropertyKey("released") -> LynxValue(1995)
    ))

    val r1 = TestRelationship(TestId(1), TestId(0), TestId(1), Option(LynxRelationshipType("ACTION_IN")), Map.empty)
    val r2 = TestRelationship(TestId(1), TestId(0), TestId(2), Option(LynxRelationshipType("ACTION_IN")), Map.empty)
    val r3 = TestRelationship(TestId(1), TestId(0), TestId(3), Option(LynxRelationshipType("ACTION_IN")), Map.empty)
    val r4 = TestRelationship(TestId(1), TestId(0), TestId(4), Option(LynxRelationshipType("ACTION_IN")), Map.empty)
    val r5 = TestRelationship(TestId(1), TestId(0), TestId(5), Option(LynxRelationshipType("ACTION_IN")), Map.empty)
    val r6 = TestRelationship(TestId(1), TestId(0), TestId(6), Option(LynxRelationshipType("ACTION_IN")), Map.empty)
    val r7 = TestRelationship(TestId(1), TestId(0), TestId(7), Option(LynxRelationshipType("ACTION_IN")), Map.empty)

    nodesInput.append(("p", NodeInput(p.labels, p.props.toSeq)))
    nodesInput.append(("m1", NodeInput(m1.labels, m1.props.toSeq)))
    nodesInput.append(("m2", NodeInput(m2.labels, m2.props.toSeq)))
    nodesInput.append(("m3", NodeInput(m3.labels, m3.props.toSeq)))
    nodesInput.append(("m4", NodeInput(m4.labels, m4.props.toSeq)))
    nodesInput.append(("m5", NodeInput(m5.labels, m5.props.toSeq)))
    nodesInput.append(("m6", NodeInput(m6.labels, m6.props.toSeq)))
    nodesInput.append(("m7", NodeInput(m7.labels, m7.props.toSeq)))

    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), r1.props.toSeq, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationsInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), r2.props.toSeq, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))
    relationsInput.append(("r3", RelationshipInput(Seq(r3.relationType.get), r3.props.toSeq, StoredNodeInputRef(r3.startNodeId), StoredNodeInputRef(r3.endNodeId))))
    relationsInput.append(("r4", RelationshipInput(Seq(r4.relationType.get), r4.props.toSeq, StoredNodeInputRef(r4.startNodeId), StoredNodeInputRef(r4.endNodeId))))
    relationsInput.append(("r5", RelationshipInput(Seq(r5.relationType.get), r5.props.toSeq, StoredNodeInputRef(r5.startNodeId), StoredNodeInputRef(r5.endNodeId))))
    relationsInput.append(("r6", RelationshipInput(Seq(r6.relationType.get), r6.props.toSeq, StoredNodeInputRef(r6.startNodeId), StoredNodeInputRef(r6.endNodeId))))
    relationsInput.append(("r7", RelationshipInput(Seq(r7.relationType.get), r7.props.toSeq, StoredNodeInputRef(r7.startNodeId), StoredNodeInputRef(r7.endNodeId))))

    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
    model.write.commit

    val records = runOnDemoGraph("" +
      """
        |MATCH (a:Person { name: 'Keanu Reeves' })
        |RETURN [(a)-[r:ACTION_IN]->(b) WHERE b:Movie | b.released] AS years
        |""".stripMargin).records()
      .map(f => f("years").asInstanceOf[LynxList].value).toArray

    val expectResult = List(1997, 1999, 2000, 2003, 2003, 2003, 1995)
    Assert.assertEquals(expectResult.length, records(0).length)
    for (i <- expectResult.indices) {
      Assert.assertEquals(LynxValue(expectResult(i)), records(0)(i))
    }
    // List Map funcs
//    expectResult.map(LynxValue.apply)
//      .zip(records(0))
//      .foreach{ case(e, a) => Assert.assertEquals(e,a)}
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
