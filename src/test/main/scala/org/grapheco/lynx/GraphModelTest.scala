package org.grapheco.lynx

import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

class GraphModelTest extends TestBase {

  runOnDemoGraph(
    """
      |Create
      |(a:person:leader{name:"bluejoe", age: 40, gender:"male"}),
      |(b:person{name:"Alice", age: 30, gender:"female"}),
      |(c{name:"Bob", age: 10, gender:"male"}),
      |(d{name:"Bob2", age: 10, gender:"male"}),
      |(a)-[:KNOWS{years:5}]->(b),
      |(b)-[:KNOWS{years:4}]->(c),
      |(c)-[:KNOWS]->(d),
      |(a)-[]->(c)
      |""".stripMargin)

  @Test
  def testGraphModel(): Unit = {
    var rs = model.paths(NodeFilter(Seq.empty, Map.empty), RelationshipFilter(Seq.empty, Map.empty), NodeFilter(Seq.empty, Map.empty), OUTGOING, (0, 0))
    Assert.assertEquals(4, rs.size)
    rs.foreach {
      item =>
        val PathTriple(startNode, rel, endNode, _) = item
        Assert.assertEquals(rel.startNodeId, startNode.id)
        Assert.assertEquals(rel.endNodeId, endNode.id)
    }

    rs = model.paths(NodeFilter(Seq.empty, Map.empty), RelationshipFilter(Seq.empty, Map.empty), NodeFilter(Seq.empty, Map.empty), INCOMING, (0, 0))
    Assert.assertEquals(4, rs.size)
    rs.foreach {
      item =>
        val PathTriple(startNode, rel, endNode, _) = item
        Assert.assertEquals(rel.startNodeId, endNode.id)
        Assert.assertEquals(rel.endNodeId, startNode.id)
    }

    rs = model.paths(NodeFilter(Seq.empty, Map.empty), RelationshipFilter(Seq.empty, Map.empty), NodeFilter(Seq.empty, Map.empty), BOTH, (0, 0))
    Assert.assertEquals(8, rs.size)
  }
}
