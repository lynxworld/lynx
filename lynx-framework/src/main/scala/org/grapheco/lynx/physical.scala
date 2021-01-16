package org.grapheco.lynx

import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}
import org.grapheco.lynx.DataFrameOps._

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
      case LogicalProcedureCall(c: UnresolvedCall) => PhysicalProcedureCall(c)
      case LogicalCreate(c: Create, in: Option[LogicalQueryClause]) => PhysicalCreate(c, in.map(plan(_)))
      case LogicalMatch(m: Match, in: Option[LogicalQueryClause]) => PhysicalMatch(m, in.map(plan(_)))
      case LogicalReturn(r: Return, in: Option[LogicalQueryClause]) => PhysicalReturn(r, in.map(plan(_)))
      case LogicalWith(w: With, in: Option[LogicalQueryClause]) => PhysicalWith(w, in.map(plan(_)))
      case LogicalQuery(LogicalSingleQuery(in)) => PhysicalSingleQuery(in.map(plan(_)))
    }
  }
}

trait AbstractPhysicalPlanNode extends PhysicalPlanNode {
  val runnerContext: CypherRunnerContext
  implicit val dataFrameOperator = runnerContext.dataFrameOperator
  implicit val expressionEvaluator = runnerContext.expressionEvaluator

  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue = expressionEvaluator.eval(expr)

  def createUnitDataFrame(items: Seq[ReturnItem], ctx: PlanExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext
    DataFrame.unit(items.map(item => item.name -> item.expression))
  }
}

case class PhysicalSingleQuery(in: Option[PhysicalPlanNode])(implicit val runnerContext: CypherRunnerContext) extends AbstractPhysicalPlanNode {
  override def execute(ctx: PlanExecutionContext): DataFrame =
    in.map(_.execute(ctx)).getOrElse(DataFrame.empty)
}

case class PhysicalProcedureCall(c: UnresolvedCall)(implicit val runnerContext: CypherRunnerContext) extends AbstractPhysicalPlanNode {

  private def checkArgs(actual: Seq[LynxValue], expected: Seq[(String, LynxType)]) = {
    if (actual.size != expected.size)
      throw WrongNumberOfArgumentsException(expected.size, actual.size)

    expected.zip(actual).foreach(x => {
      val ((name, ctype), value) = x
      if (value != LynxNull && value.cypherType != ctype)
        throw WrongArgumentException(name, ctype, value)
    })
  }

  override def execute(ctx: PlanExecutionContext): DataFrame = {
    val UnresolvedCall(Namespace(parts: List[String]), ProcedureName(name: String), declaredArguments: Option[Seq[Expression]], declaredResult: Option[ProcedureResult]) = c

    val df = runnerContext.graphModel.getProcedure(parts, name) match {
      case Some(procedure) =>
        val args = declaredArguments match {
          case Some(args) => args.map(eval(_)(ctx.expressionContext))
          case None => procedure.inputs.map(arg => ctx.expressionContext.params.getOrElse(arg._1, LynxNull))
        }

        checkArgs(args, procedure.inputs)
        val records = procedure.call(args)

        DataFrame(procedure.outputs, () => records.iterator)

      case None => throw UnknownProcedureException(parts, name)
    }

    declaredResult match {
      case Some(ProcedureResult(items: IndexedSeq[ProcedureResultItem], where: Option[Where])) =>
        df.select(
          items.map(
            item => {
              val ProcedureResultItem(output, Variable(varname)) = item
              varname -> output.map(_.name)
            }
          )
        ).filter(where.map(_.expression))(ctx.expressionContext)

      case None => df
    }
  }
}

case class PhysicalCreate(c: Create, in: Option[PhysicalPlanNode])(implicit val runnerContext: CypherRunnerContext) extends AbstractPhysicalPlanNode {
  override def execute(ctx: PlanExecutionContext): DataFrame = {
    //create
    val nodes = ArrayBuffer[(Option[LogicalVariable], NodeInput)]()
    val rels = ArrayBuffer[(Option[LogicalVariable], RelationshipInput)]()
    implicit val ec = ctx.expressionContext

    c.pattern.patternParts.foreach {
      case EveryPath(NodePattern(variable: Option[LogicalVariable], labels, properties, _)) =>
        nodes += variable -> NodeInput(labels.map(_.name), properties.map {
          case MapExpression(items) =>
            items.map({
              case (k, v) => k.name -> eval(v)
            })
        }.getOrElse(Seq.empty))

      case EveryPath(RelationshipChain(element,
      RelationshipPattern(
      variable: Option[LogicalVariable],
      types: Seq[RelTypeName],
      length: Option[Option[Range]],
      properties: Option[Expression],
      direction: SemanticDirection,
      legacyTypeSeparator: Boolean,
      baseRel: Option[LogicalVariable]), rightNode)) =>
        def nodeRef(pe: PatternElement): NodeInputRef = {
          pe match {
            case NodePattern(variable, _, _, _) =>
              nodes.toMap.get(variable).map(ContextualNodeInputRef(_)).getOrElse(throw UnresolvableVarException(variable))
          }
        }

        rels += variable -> RelationshipInput(types.map(_.name), properties.map {
          case MapExpression(items) =>
            items.map({
              case (k, v) => k.name -> eval(v)
            })
        }.getOrElse(Seq.empty[(String, LynxValue)]),
          nodeRef(element),
          nodeRef(rightNode)
        )

      case _ =>
    }

    runnerContext.graphModel.createElements(
      nodes.map(x => x._1.map(_.name) -> x._2).toArray,
      rels.map(x => x._1.map(_.name) -> x._2).toArray,
      (nodesCreated: Map[Option[String], LynxNode], relsCreated: Map[Option[String], LynxRelationship]) => {

        val schema = nodesCreated.map(_._1).flatMap(_.toSeq).map(_ -> CTNode) ++
          relsCreated.map(_._1).flatMap(_.toSeq).map(_ -> CTRelationship)

        DataFrame(schema.toSeq, () => Iterator.single(nodesCreated.filter(_._1.isDefined).map(_._2).toSeq ++
          relsCreated.filter(_._1.isDefined).map(_._2)))
      })
  }
}

case class NodeInput(labels: Seq[String], props: Seq[(String, LynxValue)]) {

}

case class RelationshipInput(types: Seq[String],
                             props: Seq[(String, LynxValue)],
                             startNodeRef: NodeInputRef,
                             endNodeRef: NodeInputRef) {

}

sealed trait NodeInputRef

case class StoredNodeInputRef(id: LynxId) extends NodeInputRef

case class ContextualNodeInputRef(node: NodeInput) extends NodeInputRef

case class PhysicalMatch(m: Match, in: Option[PhysicalPlanNode])(implicit val runnerContext: CypherRunnerContext) extends AbstractPhysicalPlanNode {
  override def execute(ctx: PlanExecutionContext): DataFrame = {
    in match {
      case None => executeMatch(m)(ctx)
      case Some(sin) => sin.execute(ctx).join(executeMatch(m)(ctx))
    }
  }

  private def executeMatch(m: Match)(implicit ctx: PlanExecutionContext): DataFrame = {
    //run match
    val Match(optional, Pattern(patternParts: Seq[PatternPart]), hints, where: Option[Where]) = m
    val dfs = patternParts.map(matchPatternPart(_)(ctx))
    val df = (dfs.drop(1)).foldLeft(dfs.head)(_.join(_))

    where match {
      case Some(Where(condition)) => df.filter(condition)(ctx.expressionContext)
      case None => df
    }
  }

  private def matchPatternPart(patternPart: PatternPart)(implicit ctx: PlanExecutionContext): DataFrame = {
    patternPart match {
      case EveryPath(element: PatternElement) => matchPattern(element)
    }
  }

  private def matchPattern(element: PatternElement)(implicit ctx: PlanExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext
    element match {
      //match (m:label1)
      case NodePattern(
      Some(var0: LogicalVariable),
      labels: Seq[LabelName],
      properties: Option[Expression],
      baseNode: Option[LogicalVariable]) =>
        DataFrame(Seq(var0.name -> CTNode), () => {
          val nodes = if (labels.isEmpty)
            runnerContext.graphModel.nodes()
          else
            runnerContext.graphModel.nodes(NodeFilter(labels.map(_.name), properties.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)))

          nodes.map(Seq(_))
        })

      //match (m:label1)-[r:type]->(n:label2)
      case RelationshipChain(
      leftNode@NodePattern(var1, labels1: Seq[LabelName], properties1: Option[Expression], baseNode1: Option[LogicalVariable]),
      RelationshipPattern(variable: Option[LogicalVariable], types: Seq[RelTypeName], length: Option[Option[Range]], properties: Option[Expression], direction: SemanticDirection, legacyTypeSeparator: Boolean, baseRel: Option[LogicalVariable]),
      rightNode@NodePattern(var2, labels2: Seq[LabelName], properties2: Option[Expression], baseNode2: Option[LogicalVariable])
      ) =>
        DataFrame(Seq(
          var1.map(_.name).getOrElse(s"__NODE_${var1.hashCode}") -> CTNode,
          variable.map(_.name).getOrElse(s"__RELATIONSHIP_${variable.hashCode}") -> CTRelationship,
          var2.map(_.name).getOrElse(s"__NODE_${var2.hashCode}") -> CTNode
        ), () => {
          runnerContext.graphModel.relationships(
            NodeFilter(labels1.map(_.name), properties1.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)),
            RelationshipFilter(types.map(_.name), properties.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)),
            NodeFilter(labels2.map(_.name), properties2.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)),
            direction).map(_.toSeq(direction))
        })

      //match ()-[]->()-...-[r:type]->(n:label2)
      case RelationshipChain(
      leftChain,
      RelationshipPattern(variable: Option[LogicalVariable], types: Seq[RelTypeName], length: Option[Option[Range]], properties: Option[Expression], direction: SemanticDirection, legacyTypeSeparator: Boolean, baseRel: Option[LogicalVariable]),
      rightNode@NodePattern(var2, labels2: Seq[LabelName], properties2: Option[Expression], baseNode2: Option[LogicalVariable])
      ) =>
        val in = matchPattern(leftChain)

        DataFrame(in.schema ++ Seq(
          variable.map(_.name).getOrElse(s"__RELATIONSHIP_${variable.hashCode}") -> CTRelationship,
          var2.map(_.name).getOrElse(s"__NODE_${var2.hashCode}") -> CTNode,
        ), () => {
          in.records.flatMap {
            record0 =>
              val len = record0.size
              runnerContext.graphModel.relationships(
                record0.last.asInstanceOf[LynxNode].id,
                RelationshipFilter(types.map(_.name), properties.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)),
                NodeFilter(labels2.map(_.name), properties2.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)),
                direction
              ).map(record0 ++ _.toSeq(direction)).filter(
                item => {
                  //(m)-[r]-(n)-[p]-(t), r!=p
                  val relIds = item.filter(_.isInstanceOf[LynxRelationship]).map(_.asInstanceOf[LynxRelationship].id)
                  relIds.size == relIds.toSet.size
                }
              )
          }
        })
    }
  }
}

trait DataFramePipe {
  def map(df: DataFrame): DataFrame
}

trait DataFramePipeBuilder {
  def build(): Iterable[DataFramePipe]
}

case class ProjectPipeBuilder(ri: ReturnItems)(implicit ctx: ExpressionContext, dfo: DataFrameOperator)
  extends DataFramePipeBuilder {
  def build(): Iterable[DataFramePipe] = Some(new DataFramePipe() {
    override def map(df: DataFrame): DataFrame =
      df.project(ri.items.map(x => x.name -> x.expression))
  })
}

case class FilterPipeBuilder(where: Option[Where])(implicit ctx: ExpressionContext, dfo: DataFrameOperator)
  extends DataFramePipeBuilder {
  def build(): Iterable[DataFramePipe] = where.map(expr =>
    new DataFramePipe() {
      override def map(df: DataFrame): DataFrame =
        df.filter(expr.expression)
    })
}

case class LimitPipeBuilder(limit: Option[Limit])(implicit ctx: ExpressionContext, dfo: DataFrameOperator, evaluator: ExpressionEvaluator)
  extends DataFramePipeBuilder {
  def build(): Iterable[DataFramePipe] = limit.map(expr =>
    new DataFramePipe() {
      override def map(df: DataFrame): DataFrame =
        df.take(evaluator.eval(expr.expression).value.asInstanceOf[Number].intValue())
    })
}

case class SkipPipeBuilder(skip: Option[Skip])(implicit ctx: ExpressionContext, dfo: DataFrameOperator, evaluator: ExpressionEvaluator)
  extends DataFramePipeBuilder {
  def build(): Iterable[DataFramePipe] = skip.map(expr =>
    new DataFramePipe() {
      override def map(df: DataFrame): DataFrame =
        df.skip(evaluator.eval(expr.expression).value.asInstanceOf[Number].intValue())
    })
}

case class SelectPipeBuilder(ri: ReturnItems)(implicit ctx: ExpressionContext, dfo: DataFrameOperator)
  extends DataFramePipeBuilder {
  def build(): Iterable[DataFramePipe] = Some(
    new DataFramePipe() {
      override def map(df: DataFrame): DataFrame =
        df.select(ri.items.map(item => item.name -> item.alias.map(_.name)))
    })
}

case class DistinctPipeBuilder(distinct: Boolean)(implicit ctx: ExpressionContext, dfo: DataFrameOperator)
  extends DataFramePipeBuilder {
  def build(): Iterable[DataFramePipe] = distinct match {
    case true => Some(new DataFramePipe() {
      override def map(df: DataFrame): DataFrame =
        df.distinct()
    })
    case false => None
  }
}

object DataFramePipe {
  def piping(start: DataFrame, builders: Seq[DataFramePipeBuilder]): DataFrame = {
    builders.flatMap(_.build()).foldLeft(start) { (df, pipe) =>
      pipe.map(df)
    }
  }
}

case class PhysicalWith(w: With, in: Option[PhysicalPlanNode])(implicit val runnerContext: CypherRunnerContext) extends AbstractPhysicalPlanNode {

  override def execute(ctx: PlanExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext
    (w, in) match {
      case (With(distinct, ReturnItems(includeExisting, items), orderBy, skip, limit: Option[Limit], where), None) =>
        createUnitDataFrame(items, ctx)

      case (With(distinct, ri: ReturnItems, orderBy, skip: Option[Skip], limit: Option[Limit], where: Option[Where]), Some(sin)) =>
        DataFramePipe.piping(
          sin.execute(ctx),
          Seq(
            ProjectPipeBuilder(ri),
            FilterPipeBuilder(where),
            SkipPipeBuilder(skip),
            LimitPipeBuilder(limit),
            SelectPipeBuilder(ri),
            DistinctPipeBuilder(distinct)
          )
        )
    }
  }
}

case class PhysicalReturn(r: Return, in: Option[PhysicalPlanNode])(implicit val runnerContext: CypherRunnerContext) extends AbstractPhysicalPlanNode {
  override def execute(ctx: PlanExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext
    (r, in) match {
      case (Return(distinct, ri: ReturnItems, orderBy, skip: Option[Skip], limit: Option[Limit], excludedNames), Some(sin)) =>
        DataFramePipe.piping(
          sin.execute(ctx),
          Seq(
            ProjectPipeBuilder(ri),
            SkipPipeBuilder(skip),
            LimitPipeBuilder(limit),
            SelectPipeBuilder(ri),
            DistinctPipeBuilder(distinct)
          )
        )
    }
  }
}

case class UnresolvableVarException(var0: Option[LogicalVariable]) extends LynxException

case class UnknownProcedureException(prefix: List[String], name: String) extends LynxException

case class WrongNumberOfArgumentsException(sizeExpected: Int, sizeActual: Int) extends LynxException

case class WrongArgumentException(argName: String, expectedType: LynxType, actualValue: LynxValue) extends LynxException