package org.grapheco.cypher

import org.grapheco.lynx.TestBase
import org.junit.Test

/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-02-28 18:52
 */
class Q_CreateUnique extends TestBase{

  @Test
  def cypher1(): Unit ={
    runOnDemoGraph("""MERGE (p:Person {name: 'Joe'})
                     |RETURN p""".stripMargin).show()
    runOnDemoGraph("""MATCH (a:Person {name: 'Joe'})
                     |CREATE UNIQUE (a)-[r:LIKES]->(b:Person {name: 'Jill'})-[r1:EATS]->(f:Food {name: 'Margarita Pizza'})
                     |RETURN a""".stripMargin).show()
    runOnDemoGraph("""MATCH (a:Person {name: 'Joe'})
                     |CREATE UNIQUE (a)-[r:LIKES]->(b:Person {name: 'Jill'})-[r1:EATS]->(f:Food {name: 'Banana'})
                     |RETURN a""".stripMargin).show()
  }
  @Test
  def cypher2(): Unit ={
    runOnDemoGraph("""MERGE (p:Person {name: 'Joe'})
                     |RETURN p""".stripMargin).show()
    runOnDemoGraph("""MATCH (a:Person {name: 'Joe'})
                     |MERGE (b:Person {name: 'Jill'})
                     |MERGE (a)-[r:LIKES]->(b)
                     |MERGE (b)-[r1:EATS]->(f:Food {name: 'Margarita Pizza'})
                     |RETURN a""".stripMargin).show()
    runOnDemoGraph("""MATCH (a:Person {name: 'Joe'})
                     |MERGE (b:Person {name: 'Jill'})
                     |MERGE (a)-[r:LIKES]->(b)
                     |MERGE (b)-[r1:EATS]->(f:Food {name: 'Banana'})
                     |RETURN a""".stripMargin).show()
  }
  @Test
  def CreateNodeIfMissing(): Unit ={
    runOnDemoGraph("""MATCH (root { name: 'root' })
                     |CREATE UNIQUE (root)-[:LOVES]-(someone)
                     |RETURN someone""".stripMargin).show()
  }
  @Test
  def CreateNodesWithValue(): Unit ={
    runOnDemoGraph("""MATCH (root { name: 'root' })
                     |CREATE UNIQUE (root)-[:X]-(leaf { name: 'D' })
                     |RETURN leaf""".stripMargin).show()
  }
  @Test
  def CreateLabeledNodeIfMissing(): Unit ={
    runOnDemoGraph("""MATCH (a { name: 'A' })
                     |CREATE UNIQUE (a)-[:KNOWS]-(c:blue)
                     |RETURN c""".stripMargin).show()
  }
  @Test
  def CreateRelationshipWithValues(): Unit ={
    runOnDemoGraph("""MATCH (root { name: 'root' })
                     |CREATE UNIQUE (root)-[r:X { since: 'forever' }]-()
                     |RETURN r""".stripMargin)
  }
  @Test
  def DescribeComplexPattern(): Unit ={
    runOnDemoGraph("""MATCH (root { name: 'root' })
                     |CREATE UNIQUE (root)-[:FOO]->(x),(root)-[:BAR]->(x)
                     |RETURN x""".stripMargin)
  }
  @Test
  def CreateRelationshipIfItIsMissing(): Unit ={
    runOnDemoGraph("""MATCH (lft { name: 'A' }),(rgt)
                     |WHERE rgt.name IN ['B', 'C']
                     |CREATE UNIQUE (lft)-[r:KNOWS]->(rgt)
                     |RETURN r""".stripMargin)
  }
  
}
