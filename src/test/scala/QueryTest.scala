import org.junit.{Before, Test}
import org.opencypher.lynx._

/**
  * Created by bluejoe on 2020/5/2.
  */
class QueryTest {
  val graph = new InMemoryPropertyGraph()

  @Before
  def setup(): Unit = {
    val node1 = graph.addNode(Set(), "name" -> "bluejoe", "age" -> 40)
    val node2 = graph.addNode(Set(), "name" -> "simba", "age" -> 10)
    val node3 = graph.addNode(Set(), "name" -> "alex", "age" -> 30)

    graph.addRelationship(node1.id, node2.id, "knows")
    graph.addRelationship(node2.id, node3.id, "knows")
  }

  @Test
  def test1(): Unit = {
    val session = new LynxCypherSession(graph, new LynxExecutor()(graph))
    val querylist = Array(
      "match (m) return m.name,m",
      "match (n) where n.age>20 return n.name",
      "match (m)-[r]-(n) where n.age>20 return m.name,n.name,r",
      "match (m)-[:knows]-(n) where n.age>20 return m.name,n.name"
    )
    for (i <- 1 to 1) {
      for (query <- querylist) {
        println(s"query: $query")
        val t1 = System.currentTimeMillis()
        val r = session.cypher(query).records
        val t2 = System.currentTimeMillis()
        println(s"fetched ${r.size} records in ${t2 - t1} ms.")
        r.show
      }
    }
  }
}
