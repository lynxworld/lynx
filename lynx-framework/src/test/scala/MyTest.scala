import org.junit.Test
import org.opencypher.lynx.{LynxRecords, LynxSession}
import org.opencypher.lynx.graph.LynxPropertyGraph
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}

class MyTest {
  val session = new LynxSession()

  @Test
  def test1(): Unit = {
    run("return 1")
    run("match (n) return n")
  }

  private def run(query: String) = {
    println(s"query: $query")
    val t1 = System.currentTimeMillis()
    val records = session.cypher(query).records
    val t2 = System.currentTimeMillis()
    println(s"fetched records in ${t2 - t1} ms.")
    records.show
    records
  }
}
