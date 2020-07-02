import cn.pandadb.lynx.{LynxGraphFactory, LynxNode, LynxRelationship, LynxSession}
import org.junit.{Before, Test}
import org.opencypher.lynx.InMemoryPropertyGraph
import org.opencypher.okapi.api.value.CypherValue.CypherMap

class SessionTest {
  val session = new LynxSession()

  @Test
  def test1(): Unit = {
    session.cypher("return 1").records.show

    val graph = session.createGraphInMemory(
      LynxNode(1, "name" -> "bluejoe", "age" -> 40),
      LynxNode(2, "name" -> "simba", "age" -> 10),
      LynxNode(3, "name" -> "alex", "age" -> 30),
      LynxRelationship(1, 1, 2, "knows"),
      LynxRelationship(2, 2, 3, "knows")
    )

    graph.cypher("match (m) return m.name,m").records.show
    graph.cypher("return 1").records.show
  }
}
