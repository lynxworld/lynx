package org.grapheco.lynx

import org.opencypher.v9_0.ast.{Create, Match, Return, ReturnItem, ReturnItems, With}
import org.opencypher.v9_0.expressions.{EveryPath, LogicalVariable, MapExpression, NodePattern, PatternElement, RelationshipChain}
import org.opencypher.v9_0.util.symbols.NumberType

import scala.collection.mutable.ArrayBuffer

trait PhysicalPlanNode {
  def execute[NodeIdType, RelationIdType](ctx: CypherExecutionContext[NodeIdType, RelationIdType]): DataFrame
}

trait PhysicalPlanner {
  def plan(logicalPlan: LogicalPlanNode): PhysicalPlanNode
}

class PhysicalPlannerImpl extends PhysicalPlanner {
  override def plan(logicalPlan: LogicalPlanNode): PhysicalPlanNode = {
    logicalPlan match {
      case LogicalCreate(c: Create, in: Option[LogicalQuerySource]) => PhysicalCreate(c, in.map(plan(_)))
      case LogicalMatch(m: Match, in: Option[LogicalQuerySource]) => PhysicalMatch(m, in.map(plan(_)))
      case LogicalReturn(r: Return, in: Option[LogicalQuerySource]) => PhysicalReturn(r, in.map(plan(_)))
      case LogicalWith(w: With, in: Option[LogicalQuerySource]) => PhysicalWith(w, in.map(plan(_)))
      case LogicalQuery(LogicalSingleQuery(in)) => PhysicalSingleQuery(in.map(plan(_)))
    }
  }
}

case class PhysicalSingleQuery(in: Option[PhysicalPlanNode]) extends PhysicalPlanNode {
  override def execute[NodeIdType, RelationIdType](ctx: CypherExecutionContext[NodeIdType, RelationIdType]): DataFrame =
    in.map(_.execute(ctx)).getOrElse(DataFrame.empty)
}

case class PhysicalCreate(c: Create, in: Option[PhysicalPlanNode]) extends PhysicalPlanNode {
  override def execute[NodeIdType, RelationIdType](ctx: CypherExecutionContext[NodeIdType, RelationIdType]): DataFrame = {
    //create
    val nodes = ArrayBuffer[(Option[LogicalVariable], IRNode)]()
    val rels = ArrayBuffer[IRRelation[NodeIdType, RelationIdType]]()

    c.pattern.patternParts.foreach {
      case EveryPath(NodePattern(variable: Option[LogicalVariable], labels, properties, _)) =>
        nodes += variable -> IRNode(labels.map(_.name), properties.map {
          case MapExpression(items) =>
            items.map({
              case (k, v) => k.name -> ctx.expressionEvaluator.eval(v)
            })
        }.getOrElse(Seq.empty))

      case EveryPath(RelationshipChain(element, relationship, rightNode)) =>
        def nodeRef(pe: PatternElement): IRNodeRef[NodeIdType] = {
          pe match {
            case NodePattern(variable, _, _, _) =>
              nodes.toMap.get(variable).map(IRContextualNodeRef[NodeIdType](_)).getOrElse(throw new UnrecognizedVarException(variable))
          }
        }

        rels += IRRelation[NodeIdType, RelationIdType](relationship.types.map(_.name), relationship.properties.map {
          case MapExpression(items) =>
            items.map({
              case (k, v) => k.name -> ctx.expressionEvaluator.eval(v)
            })
        }.getOrElse(Seq.empty[(String, CypherValue)]),
          nodeRef(element),
          nodeRef(rightNode)
        )

      case _ =>
    }

    ctx.graphProvider.createElements(nodes.map(_._2).toArray, rels.toArray)
    DataFrame.empty
  }
}

case class IRNode(labels: Seq[String], props: Seq[(String, CypherValue)]) {

}

case class IRRelation[NodeId, RelationId](types: Seq[String], props: Seq[(String, CypherValue)], startNodeRef: IRNodeRef[NodeId], endNodeRef: IRNodeRef[NodeId]) {

}

sealed trait IRNodeRef[NodeId]

case class IRStoredNodeRef[NodeId](id: NodeId) extends IRNodeRef[NodeId]

case class IRContextualNodeRef[NodeId](node: IRNode) extends IRNodeRef[NodeId]

case class PhysicalMatch(m: Match, in: Option[PhysicalPlanNode]) extends PhysicalPlanNode {
  override def execute[NodeIdType, RelationIdType](ctx: CypherExecutionContext[NodeIdType, RelationIdType]): DataFrame = {
    //run match
    ???
  }
}

case class PhysicalWith(w: With, in: Option[PhysicalPlanNode]) extends PhysicalPlanNode {
  override def execute[NodeIdType, RelationIdType](ctx: CypherExecutionContext[NodeIdType, RelationIdType]): DataFrame = {
    ???
  }
}

case class PhysicalReturn(r: Return, in: Option[PhysicalPlanNode]) extends PhysicalPlanNode {
  override def execute[NodeIdType, RelationIdType](ctx: CypherExecutionContext[NodeIdType, RelationIdType]): DataFrame = {
    (r, in) match {
      case (r, Some(sin)) => val df = sin.execute(ctx)
        ctx.dataFrameOperator.select(df, r.returnItems.items.map(item => item.name -> item.alias.map(_.name)))
      case (
        Return(distinct, ReturnItems(includeExisting, items: Seq[ReturnItem]), orderBy, skip, limit, excludedNames), None) =>

        val schema = items.map(item => {
          val name = item.alias.map(_.name).getOrElse(item.expression.asCanonicalStringVal)
          val ctype = NumberType.instance
          name -> ctype
        })

        val records = Some(
          items.map(item => {
            ctx.expressionEvaluator.eval(item.expression)
          })
        )

        DataFrame(schema, records)
    }
  }
}

class UnrecognizedVarException(var0: Option[LogicalVariable]) extends LynxException
