import org.junit.Test
import org.opencypher.lynx.{LynxNode, LynxRelationship, SingleGraphCypherSession}
import org.opencypher.okapi.api.value.CypherValue.CypherMap

/**
  * Created by bluejoe on 2020/5/2.
  */
class QueryTest {
  @Test
  def test1(): Unit = {
    val graph = new LynxPropertyGraph() {
      val nodes = Array(
        LynxNode(1, Set(), CypherMap("name" -> "bluejoe", "age" -> 40)),
        LynxNode(2, Set(), CypherMap("name" -> "simba", "age" -> 10))
      ).iterator

      val rels = Array(
        LynxRelationship(1, 1, 2, "knows", CypherMap())
      ).iterator
    }

    val session = new SingleGraphCypherSession(graph, new LynxExecutor(graph))
    val r = session.cypher("match (n) where n.age>20 return n.name,n.age ").records.iterator
    while (r.hasNext) {
      println(r.next())
    }
  }

}
