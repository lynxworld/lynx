package org.grapheco.cypher.syntax

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.runner.GraphModel
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.structural._
import org.junit.{Assert, Before, BeforeClass, Test}

import scala.collection.mutable.ArrayBuffer

/*
create (a:Person{name:'Charlie Sheen',realName:'Carlos Irwin Estevez'})
create (a:Person{name:'Martin Sheen'})
create (m:Movie{year:1987,title:'Wall Street'})
create (m:Movie{year:1979,title:'Apocalypse Now'})
create (m:Movie{year:1984,title:'Red Dawn'})

match (a:Person),(m:Movie)
where a.name='Martin Sheen' and m.title='Wall Street'
create (a)-[r:ACTION_IN]->(m)

match (a:Person),(m:Movie)
where a.name='Martin Sheen' and m.title='Apocalypse Now'
create (a)-[r:ACTION_IN]->(m)

match (a:Person),(m:Movie)
where a.name='Charlie Sheen' and m.title='Wall Street'
create (a)-[r:ACTION_IN]->(m)

match (a:Person),(m:Movie)
where a.name='Charlie Sheen' and m.title='Apocalypse Now'
create (a)-[r:ACTION_IN]->(m)

match (a:Person),(m:Movie)
where a.name='Charlie Sheen' and m.title='Red Dawn'
create (a)-[r:ACTION_IN]->(m)
*/

class TestMap extends TestBase {
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationshipsInput = ArrayBuffer[(String, RelationshipInput)]()

  val persons = List(
    TestNode(TestId(1), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name") -> LynxValue("Martin Sheen"))),
    TestNode(TestId(2), Seq(LynxNodeLabel("Person")), Map(LynxPropertyKey("name") -> LynxValue("Charlie Sheen"), LynxPropertyKey("realName") -> LynxValue("Carlos Irwin Estevez")))
  )

  val movies = List(
    TestNode(TestId(3), Seq(LynxNodeLabel("Movie")), Map(LynxPropertyKey("year") -> LynxValue(1987), LynxPropertyKey("title") -> LynxValue("Wall Street"))),
    TestNode(TestId(4), Seq(LynxNodeLabel("Movie")), Map(LynxPropertyKey("year") -> LynxValue(1979), LynxPropertyKey("title") -> LynxValue("Apocalypse Now"))),
    TestNode(TestId(5), Seq(LynxNodeLabel("Movie")), Map(LynxPropertyKey("year") -> LynxValue(1984), LynxPropertyKey("title") -> LynxValue("Red Dawn")))
  )

  val relationships = List(
    TestRelationship(TestId(1), TestId(1), TestId(3), Option(LynxRelationshipType("ACTED_IN")), Map.empty),
    TestRelationship(TestId(2), TestId(1), TestId(4), Option(LynxRelationshipType("ACTED_IN")), Map.empty),
    TestRelationship(TestId(3), TestId(2), TestId(3), Option(LynxRelationshipType("ACTED_IN")), Map.empty),
    TestRelationship(TestId(4), TestId(2), TestId(4), Option(LynxRelationshipType("ACTED_IN")), Map.empty),
    TestRelationship(TestId(5), TestId(2), TestId(5), Option(LynxRelationshipType("ACTED_IN")), Map.empty)
  )

  @Before
  def init(): Unit = {
    all_nodes.clear()
    all_rels.clear()

    for (i <- 0 to persons.length - 1) {
      nodesInput.append(("p" + i, NodeInput(persons(i).labels, persons(i).props.toSeq)))
    }

    for (i <- 0 to movies.length - 1) {
      nodesInput.append(("m" + i, NodeInput(movies(i).labels, movies(i).props.toSeq)))
    }

    for (i <- 0 to relationships.length - 1) {
      relationshipsInput.append(("r" + i, RelationshipInput(Seq(relationships(i).relationType.get), Seq.empty, StoredNodeInputRef(relationships(i).startNodeId), StoredNodeInputRef(relationships(i).endNodeId))))
    }

    model.write.createElements(nodesInput, relationshipsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
  }


  @Test
  def literalMap(): Unit = {
    val records = runOnDemoGraph("RETURN { key: 'Value', listKey: [{ inner: 'Map1' }, { inner: 'Map2' }]}")
      .records().map(f => f("{ key: 'Value', listKey: [{ inner: 'Map1' }, { inner: 'Map2' }]}").asInstanceOf[LynxMap].value).toArray

    val expectResult = Map("key" -> LynxValue("Value"), "listKey" -> LynxList(List(LynxMap(Map("inner" -> LynxValue("Map1"))), LynxMap(Map("inner" -> LynxValue("Map2"))))))
    Assert.assertEquals(expectResult, records(0))
  }

  @Test
  def mapProjectionEx1(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (actor:Person { name: 'Charlie Sheen' })-[:ACTED_IN]->(movie:Movie)
        |RETURN actor { .name, .realName, movies: collect(movie { .title, .year })}
        |""".stripMargin)
      .records().map(f => f("actor").asInstanceOf[LynxMap].value).toArray
    Assert.assertEquals(1, records.length)
  }

  @Test
  def mapProjectionEx2(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie)
        |WITH actor, count(movie) AS nrOfMovies
        |RETURN actor { .name, nrOfMovies }
        |""".stripMargin)
      .records().map(f => f("actor").asInstanceOf[LynxMap].value).toArray
    Assert.assertEquals(2, records.length)
  }

  @Test
  def mapProjectionEx3(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (actor:Person { name: 'Charlie Sheen' })
        |RETURN actor { .*, .age }
        |""".stripMargin)
      .records().map(f => f("actor").asInstanceOf[LynxMap].value).toArray
    Assert.assertEquals(1, records.length)
  }
}
