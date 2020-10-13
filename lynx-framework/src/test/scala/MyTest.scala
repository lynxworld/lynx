import org.junit.Test
import org.opencypher.lynx.{LynxRecords, LynxSession}

class MyTest {
  val session = new LynxSession()

  @Test
  def test1(): Unit = {
    run("return 1")
    run("match (n) return n")
  }

  private def run(query: String):LynxRecords = {
    println(s"query: $query")
    val t1 = System.currentTimeMillis()
    val records = session.cypher(query).records
    val t2 = System.currentTimeMillis()
    println(s"fetched records in ${t2 - t1} ms.")
    records.show
    records
  }
}
