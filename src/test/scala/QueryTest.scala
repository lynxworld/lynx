
import org.junit.{Before, Test}
import org.opencypher.lynx.{InMemoryPropertyGraph, LynxCypherSession, LynxQueryPlanner}

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

  private def run(query: String) = {
    val session = new LynxCypherSession(graph, new LynxQueryPlanner()(graph))

    println(s"query: $query")
    val t1 = System.currentTimeMillis()
    val records = session.cypher(query).records
    val t2 = System.currentTimeMillis()
    println(s"fetched ${records.size} records in ${t2 - t1} ms.")
    records.show
    records
  }

  @Test
  def testBlob(): Unit = {
    run("return <file:///etc/profile>")
  }

  @Test
  def testNormal(): Unit = {
    run("return 1")
    run("return 2>1")
    run("match (m) return m.name,m")
    run("match (n) where n.age>20 return n.name")
    run("match (m)-[r]-(n) where n.age>20 return m.name,n.name,r")
    run("match (m)-[:knows]-(n) where n.age>20 return m.name,n.name")
  }
}
