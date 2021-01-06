import org.grapheco.commons.util.Profiler
import org.grapheco.lynx.{CypherResult, CypherRunner, GraphProvider, IRNode, IRRelation}
import org.junit.Test

class SessionTest {
  Profiler.enableTiming = true

  val session = new CypherRunner() {
    override val graphProvider = new GraphProvider[Long, Long] {
      override def createElements(nodes: Array[IRNode], rels: Array[IRRelation[Long, Long]]): Unit = ???
    }
  }

  def query(query: String, param: Map[String, Any] = Map.empty[String, Any]) = {
    println(s"query: $query")
    Profiler.timing() {
      val rs = session.cypher(query, param)
      rs.show()
    }
  }

  @Test
  def test(): Unit = {
    query("return 1")
  }
}
