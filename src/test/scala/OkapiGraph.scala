import org.opencypher.lynx.{LynxCypherRecords, LynxNode, LynxRelationship, QueryPlanExecutor}
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.ir.impl.QueryLocalCatalog
import org.opencypher.okapi.logical.impl._

/**
  * Created by bluejoe on 2020/4/28.
  */

abstract class LynxPropertyGraph extends PropertyGraph {
  val nodes: Iterator[LynxNode]
  val rels: Iterator[LynxRelationship]

  override def nodes(name: String, nodeCypherType: CTNode, exactLabelMatch: Boolean): LynxCypherRecords =
    LynxCypherRecords(Map(name -> CTNode), nodes.map(x => CypherMap(name -> x)))

  var _session: CypherSession = null

  def setSession(session: CypherSession) = _session = session

  override def session: CypherSession = _session

  override def relationships(name: String, relCypherType: CTRelationship): LynxCypherRecords =
    LynxCypherRecords(Map(name -> CTRelationship), rels.map(x => CypherMap(name -> x)))

  override def unionAll(others: PropertyGraph*): PropertyGraph = ???

  override def schema: PropertyGraphSchema = PropertyGraphSchema.empty.withNodePropertyKeys("person")("name" -> CTString, "age" -> CTInteger)
}

trait LynxQueryPipe {
  def getRecords(): LynxCypherRecords
}

class LynxExecutor(propertyGraph: LynxPropertyGraph) extends QueryPlanExecutor {

  override def execute(parameters: CypherMap, logicalPlan: LogicalOperator, queryLocalCatalog: QueryLocalCatalog): CypherResult =
    new CypherResult {
      type Records = LynxCypherRecords

      type Graph = PropertyGraph

      override def getGraph: Option[Graph] = Some(propertyGraph)

      override def getRecords: Option[Records] = Some(translate(logicalPlan).getRecords())

      override def plans: CypherQueryPlans = new CypherQueryPlans() {
        override def logical: String = "logical"

        override def relational: String = "relational"
      }

      override def show(implicit options: PrintOptions): Unit = {}

      def translate(op: LogicalOperator): LynxQueryPipe = {
        op match {
          case Select(fields: List[Var], in, solved) =>
            SelectPipe(translate(in), fields)

          case Filter(expr: Expr, in: LogicalOperator, solved: SolvedQueryModel) =>
            FilterPipe(translate(in), expr, parameters)

          case Project(projectExpr: (Expr, Option[Var]), in: LogicalOperator, solved: SolvedQueryModel) =>
            ProjectPipe(translate(in), projectExpr)

          case PatternScan(pattern: Pattern, mapping: Map[Var, PatternElement], in: LogicalOperator, solved: SolvedQueryModel) =>
            PatternScanPipe(propertyGraph, pattern: Pattern, mapping)
        }
      }
    }
}

case class SelectPipe(in: LynxQueryPipe, fields: List[Var]) extends LynxQueryPipe {
  override def getRecords(): LynxCypherRecords = in.getRecords().select(fields.map(_.name).toArray)
}

case class ProjectPipe(in: LynxQueryPipe, projectExpr: (Expr, Option[Var])) extends LynxQueryPipe {
  override def getRecords(): LynxCypherRecords = {
    val nodes = in.getRecords()
    val (ex1, var1) = projectExpr
    nodes.project(ex1)
  }
}

case class FilterPipe(in: LynxQueryPipe, expr: Expr, parameters: CypherMap) extends LynxQueryPipe {
  override def getRecords(): LynxCypherRecords = {
    in.getRecords().filter(expr, parameters)
  }
}

case class PatternScanPipe(graph: LynxPropertyGraph, pattern: Pattern, mapping: Map[Var, PatternElement]) extends LynxQueryPipe {
  override def getRecords(): LynxCypherRecords = pattern match {
    case np: NodePattern => graph.nodes(mapping.head._1.name, CTNode, true)
  }
}