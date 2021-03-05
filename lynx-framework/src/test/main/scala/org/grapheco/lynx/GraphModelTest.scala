package org.grapheco.lynx

import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

class GraphModelTest extends TestBase {
  @Test
  def testGraphModel(): Unit = {
    var rs = model.paths(NodeFilter(Seq.empty, Map.empty), RelationshipFilter(Seq.empty, Map.empty), NodeFilter(Seq.empty, Map.empty), OUTGOING)
    Assert.assertEquals(3, rs.size)
    rs.foreach {
      item =>
        val PathTriple(startNode, rel, endNode, _) = item
        Assert.assertEquals(rel.startNodeId, startNode.id)
        Assert.assertEquals(rel.endNodeId, endNode.id)
    }

    rs = model.paths(NodeFilter(Seq.empty, Map.empty), RelationshipFilter(Seq.empty, Map.empty), NodeFilter(Seq.empty, Map.empty), INCOMING)
    Assert.assertEquals(3, rs.size)
    rs.foreach {
      item =>
        val PathTriple(startNode, rel, endNode, _) = item
        Assert.assertEquals(rel.startNodeId, endNode.id)
        Assert.assertEquals(rel.endNodeId, startNode.id)
    }

    rs = model.paths(NodeFilter(Seq.empty, Map.empty), RelationshipFilter(Seq.empty, Map.empty), NodeFilter(Seq.empty, Map.empty), BOTH)
    Assert.assertEquals(6, rs.size)
  }
}
