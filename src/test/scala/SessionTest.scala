import cn.pandadb.lynx.{LynxNode, LynxRelationship, LynxSession}
import org.junit.Test

class SessionTest {
  val session = new LynxSession()
  val graph = session.createGraphInMemory(
    LynxNode(1, "name" -> "bluejoe", "age" -> 40),
    LynxNode(2, "name" -> "simba", "age" -> 10),
    LynxNode(3, "name" -> "alex", "age" -> 30),
    LynxRelationship(1, 1, 2, "knows"),
    LynxRelationship(2, 2, 3, "knows")
  )

  private def run(query: String) = {
    println(s"query: $query")
    val t1 = System.currentTimeMillis()
    val records = graph.cypher(query).records
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
