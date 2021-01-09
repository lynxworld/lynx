package org.grapheco.lynx

import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship, CypherType}

import scala.collection.mutable.ArrayBuffer

trait PhysicalPlanNode {
  def execute(ctx: PlanExecutionContext): DataFrame
}

trait PhysicalPlanner {
  def plan(logicalPlan: LogicalPlanNode): PhysicalPlanNode
}

class PhysicalPlannerImpl()(implicit runnerContext: CypherRunnerContext) extends PhysicalPlanner {
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


trait AbstractPhysicalPlanNode extends PhysicalPlanNode {
  def typeOf(expr: Expression): CypherType =
    expr match {
      case Parameter(name, parameterType) => parameterType
    }

  val runnerContext: CypherRunnerContext

  def dataFrameOperator: DataFrameOperator = runnerContext.dataFrameOperator

  def eval(expr: Expression)(implicit ec: ExpressionContext): CypherValue = runnerContext.expressionEvaluator.eval(expr)(ec)

  def createUnitDataFrame(items: Seq[ReturnItem], ctx: PlanExecutionContext): DataFrame = {
    val schema = items.map(item => {
      val name = item.alias.map(_.name).getOrElse(item.expression.asCanonicalStringVal)
      val ctype = typeOf(item.expression)
      name -> ctype
    })

    DataFrame(schema, () => Iterator.single(
      items.map(item => {
        eval(item.expression)(ExpressionContextImpl(ctx.queryParameters))
      })))
  }
}

case class PhysicalSingleQuery(in: Option[PhysicalPlanNode])(implicit val runnerContext: CypherRunnerContext) extends AbstractPhysicalPlanNode {
  override def execute(ctx: PlanExecutionContext): DataFrame =
    in.map(_.execute(ctx)).getOrElse(DataFrame.empty)
}

case class PhysicalCreate(c: Create, in: Option[PhysicalPlanNode])(implicit val runnerContext: CypherRunnerContext) extends AbstractPhysicalPlanNode {
  override def execute(ctx: PlanExecutionContext): DataFrame = {
    //create
    val nodes = ArrayBuffer[(Option[LogicalVariable], IRNode)]()
    val rels = ArrayBuffer[IRRelation]()
    implicit val ec = ExpressionContextImpl(ctx.queryParameters)

    c.pattern.patternParts.foreach {
      case EveryPath(NodePattern(variable: Option[LogicalVariable], labels, properties, _)) =>
        nodes += variable -> IRNode(labels.map(_.name), properties.map {
          case MapExpression(items) =>
            items.map({
              case (k, v) => k.name -> eval(v)
            })
        }.getOrElse(Seq.empty))

      case EveryPath(RelationshipChain(element, relationship, rightNode)) =>
        def nodeRef(pe: PatternElement): IRNodeRef = {
          pe match {
            case NodePattern(variable, _, _, _) =>
              nodes.toMap.get(variable).map(IRContextualNodeRef(_)).getOrElse(throw new UnrecognizedVarException(variable))
          }
        }

        rels += IRRelation(relationship.types.map(_.name), relationship.properties.map {
          case MapExpression(items) =>
            items.map({
              case (k, v) => k.name -> eval(v)
            })
        }.getOrElse(Seq.empty[(String, CypherValue)]),
          nodeRef(element),
          nodeRef(rightNode)
        )

      case _ =>
    }

    runnerContext.graphProvider.createElements(nodes.map(_._2).toArray, rels.toArray)
    DataFrame.empty
  }
}

case class IRNode(labels: Seq[String], props: Seq[(String, CypherValue)]) {

}

case class IRRelation(types: Seq[String], props: Seq[(String, CypherValue)], startNodeRef: IRNodeRef, endNodeRef: IRNodeRef) {

}

sealed trait IRNodeRef

case class IRStoredNodeRef(id: CypherId) extends IRNodeRef

case class IRContextualNodeRef(node: IRNode) extends IRNodeRef

case class PhysicalMatch(m: Match, in: Option[PhysicalPlanNode])(implicit val runnerContext: CypherRunnerContext) extends AbstractPhysicalPlanNode {
  override def execute(ctx: PlanExecutionContext): DataFrame = {
    //run match
    val Match(optional, Pattern(patternParts: Seq[PatternPart]), hints, where: Option[Where]) = m
    patternParts match {
      case Seq(EveryPath(element: PatternElement)) =>
        patternMatch(element)(ctx)
    }
  }

  private def patternMatch(element: PatternElement)(ctx: PlanExecutionContext): DataFrame = {
    element match {
      case NodePattern(
      Some(var0: LogicalVariable),
      labels: Seq[LabelName],
      properties: Option[Expression],
      baseNode: Option[LogicalVariable]) =>
        DataFrame(Seq(var0.name -> CTNode), () => {
          val nodes = runnerContext.graphProvider.nodes(labels.map(_.name))
          nodes.map(Seq(_))
        })

      case RelationshipChain(
      leftNode@NodePattern(var1, labels1: Seq[LabelName], properties1: Option[Expression], baseNode1: Option[LogicalVariable]),
      RelationshipPattern(variable: Option[LogicalVariable], types: Seq[RelTypeName], length: Option[Option[Range]], properties: Option[Expression], direction: SemanticDirection, legacyTypeSeparator: Boolean, baseRel: Option[LogicalVariable]),
      rightNode@NodePattern(var2, labels2: Seq[LabelName], properties2: Option[Expression], baseNode2: Option[LogicalVariable])
      ) =>
        DataFrame((variable.map(_.name -> CTRelationship) ++ ((var1 ++ var2).map(_.name -> CTNode))).toSeq, () => {
          val rels: Iterator[(CypherRelationship, Option[CypherNode], Option[CypherNode])] =
            runnerContext.graphProvider.rels(types.map(_.name), labels1, labels2, var1.isDefined, var2.isDefined)
          rels.flatMap {
            rel => {
              val (v0, v1, v2) = rel
              direction match {
                case BOTH =>
                  Iterator.apply(
                    Seq(v0) ++ var1.map(_ => v1.get) ++ var2.map(_ => v2.get),
                    Seq(v0) ++ var1.map(_ => v2.get) ++ var2.map(_ => v1.get)
                  )
                case INCOMING =>
                  Iterator.single(Seq(v0) ++ var1.map(_ => v2.get) ++ var2.map(_ => v1.get))
                case OUTGOING =>
                  Iterator.single(Seq(v0) ++ var1.map(_ => v1.get) ++ var2.map(_ => v2.get))
              }
            }
          }
        })
    }
  }
}

case class PhysicalWith(w: With, in: Option[PhysicalPlanNode])(implicit val runnerContext: CypherRunnerContext) extends AbstractPhysicalPlanNode {
  override def execute(ctx: PlanExecutionContext): DataFrame = {
    (w, in) match {
      case (With(distinct, ReturnItems(includeExisting, items), orderBy, skip, limit: Option[Limit], where), None) =>
        createUnitDataFrame(items, ctx)
    }
  }
}

case class PhysicalReturn(r: Return, in: Option[PhysicalPlanNode])(implicit val runnerContext: CypherRunnerContext) extends AbstractPhysicalPlanNode {
  override def execute(ctx: PlanExecutionContext): DataFrame = {
    (r, in) match {
      case (Return(distinct, ReturnItems(includeExisting, items), orderBy, skip, limit, excludedNames), Some(sin)) =>
        val df = sin.execute(ctx)
        runnerContext.dataFrameOperator.select(df, items.map(item => item.name -> item.alias.map(_.name)))

      case (Return(distinct, ReturnItems(includeExisting, items), orderBy, skip, limit, excludedNames), None) =>
        createUnitDataFrame(items, ctx)
    }
  }
}

class UnrecognizedVarException(var0: Option[LogicalVariable]) extends LynxException
