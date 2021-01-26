package org.grapheco.lynx

import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions.{NodePattern, RelationshipChain, _}
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}
import org.grapheco.lynx.DataFrameOps._
import scala.collection.mutable.ArrayBuffer

trait PhysicalPlanNode extends TreeNode {
  def execute(ctx: PlanExecutionContext): DataFrame
}

trait PhysicalPlanner {
  def plan(logicalPlan: LPTNode): PhysicalPlanNode
}

class PhysicalPlannerImpl()(implicit runnerContext: CypherRunnerContext) extends PhysicalPlanner {
  override def plan(logicalPlan: LPTNode): PhysicalPlanNode = {
    logicalPlan match {
      case LPTProcedureCall(c: UnresolvedCall) => PhysicalProcedureCall(c)
      //case LogicalCreate(c: Create, in: Option[PhysicalPlanNode]) => PhysicalCreate(c, in.map(plan(_)))
      //case LogicalMatch(m: Match, in: Option[LogicalQueryClause]) => PhysicalMatch(m, in.map(plan(_)))
      //case LogicalReturn(r: Return, in: Option[LogicalQueryClause]) => PhysicalReturn(r, in.map(plan(_)))
      //case LogicalWith(w: With, in: Option[LogicalQueryClause]) => PhysicalWith(w, in.map(plan(_)))
      //case LogicalQuery(LogicalSingleQuery(in)) => PhysicalSingleQuery(in.map(plan(_)))
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
  override val children: Seq[TreeNode] = in.toSeq

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
        implicit val ec = ctx.expressionContext
        DataFramePipe.piping(df, Seq(SelectPipeBuilder(items.map(
          item => {
            val ProcedureResultItem(output, Variable(varname)) = item
            varname -> output.map(_.name)
          }
        )), FilterPipeBuilder(where)))
      case None => df
    }
  }
}

trait CreateElement

case class CreateNode(varName: String, labels3: Seq[LabelName], properties3: Option[Expression]) extends CreateElement

case class CreateRelationship(varName: String, types: Seq[RelTypeName], properties: Option[Expression], varNameLeftNode: String, varNameRightNode: String) extends CreateElement

case class PhysicalCreate(c: Create, in: Option[PhysicalPlanNode])(implicit val runnerContext: CypherRunnerContext) extends AbstractPhysicalPlanNode {
  override val children: Seq[TreeNode] = in.toSeq

  override def execute(ctx: PlanExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext
    val df = in.map(_.execute(ctx)).getOrElse(DataFrame.unit(Seq.empty))
    val definedVars = df.schema.map(_._1).toSet
    val (schemaLocal, ops) = c.pattern.patternParts.foldLeft((Seq.empty[(String, LynxType)], Seq.empty[CreateElement])) {
      (result, part) =>
        val (schema1, ops1) = result
        part match {
          case EveryPath(element) =>
            element match {
              //create (n)
              case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) =>
                val leftNodeName = var1.map(_.name).getOrElse(s"__NODE_${element.hashCode}")
                (schema1 :+ (leftNodeName -> CTNode)) ->
                  (ops1 :+ CreateNode(leftNodeName, labels1, properties1))

              //create (m)-[r]-(n)
              case chain: RelationshipChain =>
                val (_, schema2, ops2) = build(chain, definedVars)
                (schema1 ++ schema2) -> (ops1 ++ ops2)
            }
        }
    }

    DataFrame(df.schema ++ schemaLocal, () =>
      df.records.map {
        record =>
          val ctxMap = df.schema.zip(record).map(x => x._1._1 -> x._2).toMap
          val nodesInput = ArrayBuffer[(String, NodeInput)]()
          val relsInput = ArrayBuffer[(String, RelationshipInput)]()

          ops.foreach(_ match {
            case CreateNode(varName: String, labels: Seq[LabelName], properties: Option[Expression]) =>
              if (!ctxMap.contains(varName) && nodesInput.find(_._1 == varName).isEmpty) {
                nodesInput += varName -> NodeInput(labels.map(_.name), properties.map {
                  case MapExpression(items) =>
                    items.map({
                      case (k, v) => k.name -> eval(v)(ec.withVars(ctxMap))
                    })
                }.getOrElse(Seq.empty))
              }

            case CreateRelationship(varName: String, types: Seq[RelTypeName], properties: Option[Expression], varNameLeftNode: String, varNameRightNode: String) =>

              def nodeInputRef(varname: String): NodeInputRef = {
                ctxMap.get(varname).map(
                  x =>
                    StoredNodeInputRef(x.asInstanceOf[LynxNode].id)
                ).getOrElse(
                  ContextualNodeInputRef(varname)
                )
              }

              relsInput += varName -> RelationshipInput(types.map(_.name), properties.map {
                case MapExpression(items) =>
                  items.map({
                    case (k, v) => k.name -> eval(v)(ec.withVars(ctxMap))
                  })
              }.getOrElse(Seq.empty[(String, LynxValue)]), nodeInputRef(varNameLeftNode), nodeInputRef(varNameRightNode))
          })

          record ++ runnerContext.graphModel.createElements(
            nodesInput,
            relsInput,
            (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
              val created = nodesCreated.toMap ++ relsCreated
              schemaLocal.map(x => created(x._1))
            })
      }
    )
  }

  //returns (varLastNode, schema, ops)
  private def build(chain: RelationshipChain, definedVars: Set[String]): (String, Seq[(String, LynxType)], Seq[CreateElement]) = {
    val RelationshipChain(
    left,
    rp@RelationshipPattern(var2: Option[LogicalVariable], types: Seq[RelTypeName], length: Option[Option[Range]], properties2: Option[Expression], direction: SemanticDirection, legacyTypeSeparator: Boolean, baseRel: Option[LogicalVariable]),
    rnp@NodePattern(var3, labels3: Seq[LabelName], properties3: Option[Expression], _)
    ) = chain

    val varRelation = var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rp.hashCode}")
    val varRightNode = var3.map(_.name).getOrElse(s"__NODE_${rnp.hashCode}")

    val schemaLocal = ArrayBuffer[(String, LynxType)]()
    val opsLocal = ArrayBuffer[CreateElement]()
    left match {
      //create (m)-[r]-(n), left=n
      case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) =>
        val varLeftNode = var1.map(_.name).getOrElse(s"__NODE_${left.hashCode}")
        if (!definedVars.contains(varLeftNode)) {
          schemaLocal += varLeftNode -> CTNode
          opsLocal += CreateNode(varLeftNode, labels1, properties1)
        }

        if (!definedVars.contains(varRightNode)) {
          schemaLocal += varRightNode -> CTNode
          opsLocal += CreateNode(varRightNode, labels3, properties3)
        }

        schemaLocal += varRelation -> CTRelationship
        opsLocal += CreateRelationship(varRelation, types, properties2, varLeftNode, varRightNode)

        (varRightNode, schemaLocal, opsLocal)

      //create (m)-[p]-(t)-[r]-(n), leftChain=(m)-[p]-(t)
      case leftChain: RelationshipChain =>
        //build (m)-[p]-(t)
        val (varLastNode, schema, ops) = build(leftChain, definedVars)
        (
          varRightNode,
          schema ++ Seq(varRelation -> CTRelationship) ++ (if (!definedVars.contains(varRightNode)) {
            Seq(varRightNode -> CTNode)
          } else {
            Seq.empty
          }),
          ops ++ (if (!definedVars.contains(varRightNode)) {
            Seq(CreateNode(varRightNode, labels3, properties3))
          } else {
            Seq.empty
          }) ++ Seq(
            CreateRelationship(varRelation, types, properties2, varLastNode, varRightNode)
          )
        )
    }
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

case class ContextualNodeInputRef(varname: String) extends NodeInputRef

case class PhysicalMatch(m: Match, in: Option[PhysicalPlanNode])(implicit val runnerContext: CypherRunnerContext) extends AbstractPhysicalPlanNode {
  override val children: Seq[TreeNode] = in.toSeq

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

    implicit val ec = ctx.expressionContext
    DataFramePipe.piping(df, Seq(FilterPipeBuilder(where)))
  }

  private def matchPatternPart(patternPart: PatternPart)(implicit ctx: PlanExecutionContext): DataFrame = {
    patternPart match {
      case EveryPath(element: PatternElement) => matchPattern(element)
    }
  }

  private def build(chain: RelationshipChain)(implicit ec: ExpressionContext): (Seq[(String, LynxType)], NodeFilter, Seq[(RelationshipFilter, NodeFilter, SemanticDirection)]) = {
    val RelationshipChain(
    left,
    rp@RelationshipPattern(variable: Option[LogicalVariable], types: Seq[RelTypeName], length: Option[Option[Range]], properties: Option[Expression], direction: SemanticDirection, legacyTypeSeparator: Boolean, baseRel: Option[LogicalVariable]),
    rnp@NodePattern(var2, labels2: Seq[LabelName], properties2: Option[Expression], baseNode2: Option[LogicalVariable])
    ) = chain

    val schema0 = Seq(variable.map(_.name).getOrElse(s"__RELATIONSHIP_${rp.hashCode}") -> CTRelationship,
      var2.map(_.name).getOrElse(s"__NODE_${rnp.hashCode}") -> CTNode)

    val filters0 = (
      RelationshipFilter(types.map(_.name), properties.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)),
      NodeFilter(labels2.map(_.name), properties2.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)),
      direction)

    left match {
      case NodePattern(var1, labels1: Seq[LabelName], properties1: Option[Expression], baseNode1: Option[LogicalVariable]) =>
        (
          Seq(var1.map(_.name).getOrElse(s"__NODE_${left.hashCode}") -> CTNode) ++ schema0,
          NodeFilter(labels1.map(_.name), properties1.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)),
          Seq(filters0)
        )

      case leftChain: RelationshipChain =>
        val (schema, startNodeFilter, chainFilters) = build(leftChain)
        (schema ++ schema0, startNodeFilter, chainFilters :+ filters0)
    }
  }

  private def matchPattern(element: PatternElement)(implicit ctx: PlanExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext
    //build schema & filter chain

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

      //match ()-[]->()-...-[r:type]->(n:label2)
      case chain: RelationshipChain =>
        val (schema, startNodeFilter, chainFilters) = build(chain)

        DataFrame(schema, () => {
          runnerContext.graphModel.paths(startNodeFilter, chainFilters: _*).map(
            triples =>
              triples.foldLeft(Seq[LynxValue](triples.head.startNode)) {
                (seq, triple) =>
                  seq ++ Seq(triple.storedRelation, triple.endNode)
              }
          )
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

case class FilterPipeBuilder(where: Option[Where])(implicit ctx: ExpressionContext, dfo: DataFrameOperator, evaluator: ExpressionEvaluator)
  extends DataFramePipeBuilder {
  def build(): Iterable[DataFramePipe] = where.map(expr =>
    new DataFramePipe() {
      override def map(df: DataFrame): DataFrame =
        df.filter {
          (record: Seq[LynxValue]) =>
            evaluator.eval(expr.expression)(ctx.withVars(df.schema.map(_._1).zip(record).toMap)) match {
              case LynxBoolean(b) => b
              case LynxNull => false
            }
        }(ctx)
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

object SelectPipeBuilder {
  def apply(ri: ReturnItems)(implicit ctx: ExpressionContext, dfo: DataFrameOperator): SelectPipeBuilder = SelectPipeBuilder(ri.items.map(item => item.name -> item.alias.map(_.name)))
}

case class SelectPipeBuilder(columns: Seq[(String, Option[String])])(implicit ctx: ExpressionContext, dfo: DataFrameOperator)
  extends DataFramePipeBuilder {
  def build(): Iterable[DataFramePipe] = Some(
    new DataFramePipe() {
      override def map(df: DataFrame): DataFrame =
        df.select(columns)
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
  override val children: Seq[TreeNode] = in.toSeq

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
  override val children: Seq[TreeNode] = in.toSeq

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