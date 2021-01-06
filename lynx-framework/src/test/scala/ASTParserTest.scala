import org.junit.Test
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.opencypher.v9_0.ast.semantics.SemanticState

class ASTParserTest {
  val printer = Prettifier(ExpressionStringifier())

  private def printAST(queryList: String*): Unit = {
    queryList.foreach(cypher => {
      println("===========================")
      println(cypher)
      try {
        val (stmt: Statement, extractedLiterals: Map[String, Any], semState: SemanticState) =
          CypherParser.process(cypher)(CypherParser.defaultContext)
        println("-------------")
        println(printer.asString(stmt))
        println("-------------")
        println(stmt.asCanonicalStringVal)
        println(s"extractedLiterals: $extractedLiterals")
      }
      catch {
        case e => println(s"Wrong statement: ${e.getMessage}")
      }
    })
  }

  @Test
  def testReturnConstant(): Unit = {
    printAST(
      "return 1",
      "return 2",
      "return 1+2",
      "with 1 as n return n",
      "unwind [1,2] as n return n",
      "unwind [1,2] as n return count(n) as c",
    )
  }

  @Test
  def testCreate(): Unit = {
    printAST(
      "create (m {name:'alex', age: 20})",
      "create (m {name:'alex', age: 20}) return m",
      "match (n) where n.name='alex' create (m {name:'alex', age: n.age+1})",
    )
  }

  @Test
  def testWrongStatement(): Unit = {
    printAST(
      "Error",
    )
  }

  @Test
  def testMatch(): Unit = {
    printAST(
      "match (n) return n",
      "match (m) return m",
      "match (n) return n.name order by n.age",
      "match (n) return n.name order by n.age limit 10",
      "match (m) with m as n return n",
      "match (m) with m as n limit 10 return n",
      "match (m) with m as n order by m.age desc limit 10 return n",
      "match (m) where m.name='alex' return m",
      "match (m {name:'alex'}) return m",
      "match (m)-[r]-(n) return m,r,n",
      "match (m)-[r:knows]-(n) return m,r,n",
      "match (m)-[r:knows]-(n) where m.name='alex' return m,r,n",
    )
  }
}
