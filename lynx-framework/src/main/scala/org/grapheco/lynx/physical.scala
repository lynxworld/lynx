package org.grapheco.lynx

import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions.{NodePattern, RelationshipChain, _}
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}

import scala.collection.mutable.ArrayBuffer

trait PPTNode extends TreeNode {
  override type SerialType = PPTNode
  override val children: Seq[PPTNode] = Seq.empty
  val schema: Seq[(String, LynxType)]

  def execute(implicit ctx: ExecutionContext): DataFrame

  def withChildren(children0: Seq[PPTNode]): PPTNode
}

trait PPTNodeTranslator {
  def translate(in: Option[PPTNode])(implicit ppc: PhysicalPlannerContext): PPTNode
}

trait AbstractPPTNode extends PPTNode {

  val plannerContext: PhysicalPlannerContext

  implicit def ops(ds: DataFrame): DataFrameOps = DataFrameOps(ds)(plannerContext.runnerContext.dataFrameOperator)

  val graphModel = plannerContext.runnerContext.graphModel

  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue = plannerContext.runnerContext.expressionEvaluator.eval(expr)

  def typeOf(expr: Expression): LynxType = plannerContext.runnerContext.expressionEvaluator.typeOf(expr, plannerContext.parameterTypes.toMap)

  def typeOf(expr: Expression, definedVarTypes: Map[String, LynxType]): LynxType = plannerContext.runnerContext.expressionEvaluator.typeOf(expr, definedVarTypes)

  def createUnitDataFrame(items: Seq[ReturnItem])(implicit ctx: ExecutionContext): DataFrame = {
    DataFrame.unit(items.map(item => item.name -> item.expression))(plannerContext.runnerContext.expressionEvaluator, ctx.expressionContext)
  }
}

trait PhysicalPlanner {
  def plan(logicalPlan: LPTNode)(implicit plannerContext: PhysicalPlannerContext): PPTNode
}

class PhysicalPlannerImpl(runnerContext: CypherRunnerContext) extends PhysicalPlanner {
  override def plan(logicalPlan: LPTNode)(implicit plannerContext: PhysicalPlannerContext): PPTNode = {
    implicit val runnerContext: CypherRunnerContext = plannerContext.runnerContext
    logicalPlan match {
      case LPTProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]]) =>
        PPTProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]])
      case lc@LPTCreate(c: Create) => PPTCreate(c)(lc.in.map(plan(_)), plannerContext)
      case ls@LPTSelect(columns: Seq[(String, Option[String])]) => PPTSelect(columns)(plan(ls.in), plannerContext)
      case lp@LPTProject(ri) => PPTProject(ri)(plan(lp.in), plannerContext)
      case lc@LPTCreateUnit(items) => PPTCreateUnit(items)(plannerContext)
      case lf@LPTFilter(expr) => PPTFilter(expr)(plan(lf.in), plannerContext)
      case ld@LPTDistinct() => PPTDistinct()(plan(ld.in), plannerContext)
      case ll@LPTLimit(expr) => PPTLimit(expr)(plan(ll.in), plannerContext)
      case ll@LPTSkip(expr) => PPTSkip(expr)(plan(ll.in), plannerContext)
      case lj@LPTJoin() => PPTJoin()(plan(lj.a), plan(lj.b), plannerContext)
      case patternMatch: LPTPatternMatch => PPTPatternMatchTranslator(patternMatch)(plannerContext).translate(None)
    }
  }
}

case class PPTPatternMatchTranslator(patternMatch: LPTPatternMatch)(implicit val plannerContext: PhysicalPlannerContext) extends PPTNodeTranslator {
  private def planPatternMatch(pm: LPTPatternMatch)(implicit ppc: PhysicalPlannerContext): PPTNode = {
    val LPTPatternMatch(headNode: NodePattern, chain: Seq[(RelationshipPattern, NodePattern)]) = pm
    chain.toList match {
      //match (m)
      case Nil => PPTNodeScan(headNode)(ppc)
      //match (m)-[r]-(n)
      case List(Tuple2(rel, rightNode)) => PPTRelationshipScan(rel, headNode, rightNode)(ppc)
      //match (m)-[r]-(n)-...-[p]-(z)
      case _ =>
        val (lastRelationship, lastNode) = chain.last
        val dropped = chain.dropRight(1)
        val part = planPatternMatch(LPTPatternMatch(headNode, dropped))(ppc)
        PPTExpandPath(lastRelationship, lastNode)(part, plannerContext)
    }
  }

  override def translate(in: Option[PPTNode])(implicit ppc: PhysicalPlannerContext): PPTNode = {
    planPatternMatch(patternMatch)(ppc)
  }
}

case class PPTJoin()(a: PPTNode, b: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = Seq(a, b)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df1 = a.execute(ctx)
    val df2 = b.execute(ctx)

    df1.join(df2)
  }

  override def withChildren(children0: Seq[PPTNode]): PPTJoin = PPTJoin()(children0.head, children0(1), plannerContext)

  override val schema: Seq[(String, LynxType)] = (a.schema ++ b.schema).distinct
}

case class PPTDistinct()(implicit in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = Seq(in)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.distinct()
  }

  override def withChildren(children0: Seq[PPTNode]): PPTDistinct = PPTDistinct()(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema
}

case class PPTLimit(expr: Expression)(implicit in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = Seq(in)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    implicit val ec = ctx.expressionContext
    df.take(eval(expr).value.asInstanceOf[Number].intValue())
  }

  override def withChildren(children0: Seq[PPTNode]): PPTLimit = PPTLimit(expr)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema
}

case class PPTSkip(expr: Expression)(implicit in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = Seq(in)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    implicit val ec = ctx.expressionContext
    df.skip(eval(expr).value.asInstanceOf[Number].intValue())
  }

  override def withChildren(children0: Seq[PPTNode]): PPTSkip = PPTSkip(expr)(children0.head, plannerContext)
}

case class PPTFilter(expr: Expression)(implicit in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = Seq(in)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val ec = ctx.expressionContext
    df.filter {
      (record: Seq[LynxValue]) =>
        eval(expr)(ec.withVars(df.schema.map(_._1).zip(record).toMap)) match {
          case LynxBoolean(b) => b
          case LynxNull => false
        }
    }(ec)
  }

  override def withChildren(children0: Seq[PPTNode]): PPTFilter = PPTFilter(expr)(children0.head, plannerContext)
}

case class PPTExpandPath(rel: RelationshipPattern, rightNode: NodePattern)(implicit in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = Seq(in)

  override def withChildren(children0: Seq[PPTNode]): PPTExpandPath = PPTExpandPath(rel, rightNode)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = {
    val RelationshipPattern(
    variable: Option[LogicalVariable],
    types: Seq[RelTypeName],
    length: Option[Option[Range]],
    properties: Option[Expression],
    direction: SemanticDirection,
    legacyTypeSeparator: Boolean,
    baseRel: Option[LogicalVariable]) = rel
    val NodePattern(var2, labels2: Seq[LabelName], properties2: Option[Expression], baseNode2: Option[LogicalVariable]) = rightNode
    val schema0 = Seq(variable.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
      var2.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode)
    in.schema ++ schema0
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val RelationshipPattern(
    variable: Option[LogicalVariable],
    types: Seq[RelTypeName],
    length: Option[Option[Range]],
    properties: Option[Expression],
    direction: SemanticDirection,
    legacyTypeSeparator: Boolean,
    baseRel: Option[LogicalVariable]) = rel
    val NodePattern(var2, labels2: Seq[LabelName], properties2: Option[Expression], baseNode2: Option[LogicalVariable]) = rightNode

    val schema0 = Seq(variable.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
      var2.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode)

    implicit val ec = ctx.expressionContext

    DataFrame(df.schema ++ schema0, () => {
      df.records.flatMap {
        record0 =>
          graphModel.expand(
            record0.last.asInstanceOf[LynxNode].id,
            RelationshipFilter(types.map(_.name), properties.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)),
            NodeFilter(labels2.map(_.name), properties2.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)),
            direction).map(triple =>
            record0 ++ Seq(triple.storedRelation, triple.endNode)).filter(
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

case class PPTNodeScan(pattern: NodePattern)(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override def withChildren(children0: Seq[PPTNode]): PPTNodeScan = PPTNodeScan(pattern)(plannerContext)

  override val schema: Seq[(String, LynxType)] = {
    val NodePattern(
    Some(var0: LogicalVariable),
    labels: Seq[LabelName],
    properties: Option[Expression],
    baseNode: Option[LogicalVariable]) = pattern
    Seq(var0.name -> CTNode)
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val NodePattern(
    Some(var0: LogicalVariable),
    labels: Seq[LabelName],
    properties: Option[Expression],
    baseNode: Option[LogicalVariable]) = pattern

    implicit val ec = ctx.expressionContext

    DataFrame(Seq(var0.name -> CTNode), () => {
      val nodes = if (labels.isEmpty)
        graphModel.nodes()
      else
        graphModel.nodes(NodeFilter(labels.map(_.name), properties.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)))

      nodes.map(Seq(_))
    })
  }
}

case class PPTRelationshipScan(rel: RelationshipPattern, leftNode: NodePattern, rightNode: NodePattern)(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override def withChildren(children0: Seq[PPTNode]): PPTRelationshipScan = PPTRelationshipScan(rel, leftNode, rightNode)(plannerContext)

  override val schema: Seq[(String, LynxType)] = {
    val RelationshipPattern(
    var2: Option[LogicalVariable],
    types: Seq[RelTypeName],
    length: Option[Option[Range]],
    props2: Option[Expression],
    direction: SemanticDirection,
    legacyTypeSeparator: Boolean,
    baseRel: Option[LogicalVariable]) = rel
    val NodePattern(var1, labels1: Seq[LabelName], props1: Option[Expression], baseNode1: Option[LogicalVariable]) = leftNode
    val NodePattern(var3, labels3: Seq[LabelName], props3: Option[Expression], baseNode3: Option[LogicalVariable]) = rightNode

    Seq(
      var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
      var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
      var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
    )
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val RelationshipPattern(
    var2: Option[LogicalVariable],
    types: Seq[RelTypeName],
    length: Option[Option[Range]],
    props2: Option[Expression],
    direction: SemanticDirection,
    legacyTypeSeparator: Boolean,
    baseRel: Option[LogicalVariable]) = rel
    val NodePattern(var1, labels1: Seq[LabelName], props1: Option[Expression], baseNode1: Option[LogicalVariable]) = leftNode
    val NodePattern(var3, labels3: Seq[LabelName], props3: Option[Expression], baseNode3: Option[LogicalVariable]) = rightNode

    implicit val ec = ctx.expressionContext

    val schema = Seq(
      var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
      var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
      var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode)

    DataFrame(schema, () => {
      graphModel.paths(
        NodeFilter(labels1.map(_.name), props1.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)),
        RelationshipFilter(types.map(_.name), props2.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)),
        NodeFilter(labels3.map(_.name), props3.map(eval(_).asInstanceOf[LynxMap].value).getOrElse(Map.empty)),
        direction).map(
        triple =>
          Seq(triple.startNode, triple.storedRelation, triple.endNode)
      )
    })
  }
}

case class PPTCreateUnit(items: Seq[ReturnItem])(val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override def withChildren(children0: Seq[PPTNode]): PPTCreateUnit = PPTCreateUnit(items)(plannerContext)

  override val schema: Seq[(String, LynxType)] =
    items.map(item => item.name -> typeOf(item.expression))

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    createUnitDataFrame(items)
  }
}

case class PPTSelect(columns: Seq[(String, Option[String])])(implicit in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = Seq(in)

  override def withChildren(children0: Seq[PPTNode]): PPTSelect = PPTSelect(columns)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = columns.map(x => x._2.getOrElse(x._1)).map(x => x -> in.schema.find(_._1 == x).get._2)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.select(columns)
  }
}

case class PPTProject(ri: ReturnItemsDef)(implicit val in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = Seq(in)

  override def withChildren(children0: Seq[PPTNode]): PPTProject = PPTProject(ri)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = ri.items.map(x => x.name -> x.expression).map { col =>
    col._1 -> typeOf(col._2, in.schema.toMap)
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.project(ri.items.map(x => x.name -> x.expression))(ctx.expressionContext)
  }

  def withReturnItems(items: Seq[ReturnItem]) = PPTProject(ReturnItems(ri.includeExisting, items)(ri.position))(in, plannerContext)
}

case class PPTProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]])(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override def withChildren(children0: Seq[PPTNode]): PPTProcedureCall = PPTProcedureCall(procedureNamespace, procedureName, declaredArguments)(plannerContext)

  private def checkArgs(actual: Seq[LynxValue], expected: Seq[(String, LynxType)]) = {
    if (actual.size != expected.size)
      throw WrongNumberOfArgumentsException(expected.size, actual.size)

    expected.zip(actual).foreach(x => {
      val ((name, ctype), value) = x
      if (value != LynxNull && value.cypherType != ctype)
        throw WrongArgumentException(name, ctype, value)
    })
  }

  override val schema: Seq[(String, LynxType)] = {
    val Namespace(parts: List[String]) = procedureNamespace
    val ProcedureName(name: String) = procedureName

    graphModel.getProcedure(parts, name) match {
      case Some(procedure) =>
        procedure.outputs

      case None => throw UnknownProcedureException(parts, name)
    }
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val Namespace(parts: List[String]) = procedureNamespace
    val ProcedureName(name: String) = procedureName

    graphModel.getProcedure(parts, name) match {
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
  }
}

trait CreateElement

case class CreateNode(varName: String, labels3: Seq[LabelName], properties3: Option[Expression]) extends CreateElement

case class CreateRelationship(varName: String, types: Seq[RelTypeName], properties: Option[Expression], varNameLeftNode: String, varNameRightNode: String) extends CreateElement

case class PPTCreate(c: Create)(implicit val in: Option[PPTNode], val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = in.toSeq

  override def withChildren(children0: Seq[PPTNode]): PPTCreate = PPTCreate(c)(children0.headOption, plannerContext)

  override val schema: Seq[(String, LynxType)] = {
    val definedVars = in.map(_.schema.map(_._1)).getOrElse(Seq.empty).toSet
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

    in.map(_.schema).getOrElse(Seq.empty) ++ schemaLocal
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext
    val df = in.map(_.execute(ctx)).getOrElse(createUnitDataFrame(Seq.empty))
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

          record ++ graphModel.createElements(
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

case class RelationshipInput(types: Seq[String], props: Seq[(String, LynxValue)], startNodeRef: NodeInputRef, endNodeRef: NodeInputRef) {

}

sealed trait NodeInputRef

case class StoredNodeInputRef(id: LynxId) extends NodeInputRef

case class ContextualNodeInputRef(varname: String) extends NodeInputRef

case class UnresolvableVarException(var0: Option[LogicalVariable]) extends LynxException

case class UnknownProcedureException(prefix: List[String], name: String) extends LynxException

case class WrongNumberOfArgumentsException(sizeExpected: Int, sizeActual: Int) extends LynxException

case class WrongArgumentException(argName: String, expectedType: LynxType, actualValue: LynxValue) extends LynxException