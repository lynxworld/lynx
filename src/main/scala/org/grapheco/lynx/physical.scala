package org.grapheco.lynx

import org.grapheco.lynx
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions.{NodePattern, RelationshipChain, _}
import org.opencypher.v9_0.util.symbols.{CTAny, CTNode, CTRelationship, CypherType, CTList}

import scala.collection.mutable
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

  val typeSystem = plannerContext.runnerContext.typeSystem
  val graphModel = plannerContext.runnerContext.graphModel
  val expressionEvaluator = plannerContext.runnerContext.expressionEvaluator
  val procedureRegistry = plannerContext.runnerContext.procedureRegistry

  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue = expressionEvaluator.eval(expr)

  def typeOf(expr: Expression): LynxType = plannerContext.runnerContext.expressionEvaluator.typeOf(expr, plannerContext.parameterTypes.toMap)

  def typeOf(expr: Expression, definedVarTypes: Map[String, LynxType]): LynxType = expressionEvaluator.typeOf(expr, definedVarTypes)

  def createUnitDataFrame(items: Seq[ReturnItem])(implicit ctx: ExecutionContext): DataFrame = {
    DataFrame.unit(items.map(item => item.name -> item.expression))(expressionEvaluator, ctx.expressionContext)
  }
}

trait PhysicalPlanner {
  def plan(logicalPlan: LPTNode)(implicit plannerContext: PhysicalPlannerContext): PPTNode
}

class DefaultPhysicalPlanner(runnerContext: CypherRunnerContext) extends PhysicalPlanner {
  override def plan(logicalPlan: LPTNode)(implicit plannerContext: PhysicalPlannerContext): PPTNode = {
    implicit val runnerContext: CypherRunnerContext = plannerContext.runnerContext
    logicalPlan match {
      case LPTProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]]) =>
        PPTProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]])
      case lc@LPTCreate(c: Create) => PPTCreateTranslator(c).translate(lc.in.map(plan(_)))(plannerContext)
      case lm@LPTMerge(m: Merge) => PPTMergeTranslator(m).translate(lm.in.map(plan(_)))(plannerContext)
      case lm@LPTMergeAction(m: Seq[MergeAction]) => PPTMergeAction(m)(plan(lm.in.get), plannerContext)
      case ld@LPTDelete(d: Delete) => PPTDelete(d)(plan(ld.in), plannerContext)
      case ls@LPTSelect(columns: Seq[(String, Option[String])]) => PPTSelect(columns)(plan(ls.in), plannerContext)
      case lp@LPTProject(ri) => PPTProject(ri)(plan(lp.in), plannerContext)
      case la@LPTAggregation(a, g) => PPTAggregation(a, g)(plan(la.in), plannerContext)
      case lc@LPTCreateUnit(items) => PPTCreateUnit(items)(plannerContext)
      case lf@LPTFilter(expr) => PPTFilter(expr)(plan(lf.in), plannerContext)
      case ld@LPTDistinct() => PPTDistinct()(plan(ld.in), plannerContext)
      case ll@LPTLimit(expr) => PPTLimit(expr)(plan(ll.in), plannerContext)
      case lo@LPTOrderBy(sortItem) => PPTOrderBy(sortItem)(plan(lo.in), plannerContext)
      case ll@LPTSkip(expr) => PPTSkip(expr)(plan(ll.in), plannerContext)
      case lj@LPTJoin(isSingleMatch) => PPTJoin(None, isSingleMatch)(plan(lj.a), plan(lj.b), plannerContext)
      case patternMatch: LPTPatternMatch => PPTPatternMatchTranslator(patternMatch)(plannerContext).translate(None)
      case li@LPTCreateIndex(labelName: LabelName, properties: List[PropertyKeyName]) => PPTCreateIndex(labelName, properties)(plannerContext)
      case sc@LPTSetClause(d) => PPTSetClauseTranslator(d.items).translate(sc.in.map(plan(_)))(plannerContext)
      case lr@LPTRemove(r) => PPTRemoveTranslator(r.items).translate(lr.in.map(plan(_)))(plannerContext)
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

case class PPTJoin(filterExpr: Option[Expression], val isSingleMatch: Boolean, bigTableIndex: Int = 1)(a: PPTNode, b: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = Seq(a, b)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df1 = a.execute(ctx)
    val df2 = b.execute(ctx)

    val df = df1.join(df2, isSingleMatch, bigTableIndex)

    if (filterExpr.nonEmpty) {
      val ec = ctx.expressionContext
      df.filter {
        (record: Seq[LynxValue]) =>
          eval(filterExpr.get)(ec.withVars(df.schema.map(_._1).zip(record).toMap)) match {
            case LynxBoolean(b) => b
            case LynxNull => false
          }
      }(ec)
    }
    else df
  }

  override def withChildren(children0: Seq[PPTNode]): PPTJoin = PPTJoin(filterExpr, isSingleMatch)(children0.head, children0(1), plannerContext)

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

case class PPTOrderBy(sortItem: Seq[SortItem])(implicit in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val schema: Seq[(String, LynxType)] = in.schema
  override val children: Seq[PPTNode] = Seq(in)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    implicit val ec: ExpressionContext = ctx.expressionContext
    /*    val sortItems:Seq[(String ,Boolean)] = sortItem.map {
          case AscSortItem(expression) => (expression.asInstanceOf[Variable].name, true)
          case DescSortItem(expression) => (expression.asInstanceOf[Variable].name, false)
        }*/
    val sortItems2: Seq[(Expression, Boolean)] = sortItem.map {
      case AscSortItem(expression) => (expression, true)
      case DescSortItem(expression) => (expression, false)
    }
    df.orderBy(sortItems2)(ec)
  }

  override def withChildren(children0: Seq[PPTNode]): PPTNode = PPTOrderBy(sortItem)(children0.head, plannerContext)
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
          case LynxNull => false //todo check logic
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
            RelationshipFilter(types.map(_.name).map(LynxRelationshipType), properties.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
            NodeFilter(labels2.map(_.name).map(LynxNodeLabel), properties2.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
            direction)
            .map(triple =>
              record0 ++ Seq(triple.storedRelation, triple.endNode))
            .filter(item => {
              //(m)-[r]-(n)-[p]-(t), r!=p
              val relIds = item.filter(_.isInstanceOf[LynxRelationship]).map(_.asInstanceOf[LynxRelationship].id)
              relIds.size == relIds.toSet.size
            })
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
      val nodes = if (labels.isEmpty) {
        graphModel.nodes(NodeFilter(Seq.empty, properties.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)))
      } else
        graphModel.nodes(NodeFilter(labels.map(_.name).map(LynxNodeLabel), properties.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)))

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

    if (length.isEmpty) {
      Seq(
        var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
        var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
        var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
      )
    }
    else {
      Seq(
        var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
        var2.map(_.name).getOrElse(s"__RELATIONSHIP_LIST_${rel.hashCode}") -> CTList(CTRelationship),
        var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
      )
    }
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

    val schema = {
      if (length.isEmpty) {
        Seq(
          var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
          var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
          var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
        )
      }
      else {
        Seq(
          var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
          var2.map(_.name).getOrElse(s"__RELATIONSHIP_LIST_${rel.hashCode}") -> CTList(CTRelationship),
          var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
        )
      }
    }

    //    length:
    //      [r:XXX] = None
    //      [r:XXX*] = Some(None) // degree 1 to MAX
    //      [r:XXX*..] =Some(Some(Range(None, None))) // degree 1 to MAX
    //      [r:XXX*..3] = Some(Some(Range(None, 3)))
    //      [r:XXX*1..] = Some(Some(Range(1, None)))
    //      [r:XXX*1..3] = Some(Some(Range(1, 3)))
    val (lowerLimit, upperLimit) = length match {
      case None => (None, None)
      case Some(None) => (Some(1), None)
      case Some(Some(Range(a, b))) => (a.map(_.value.toInt), b.map(_.value.toInt))
    }

    DataFrame(schema,
      () => {
        graphModel.paths(
          NodeFilter(labels1.map(_.name).map(LynxNodeLabel), props1.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
          RelationshipFilter(types.map(_.name).map(LynxRelationshipType), props2.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
          NodeFilter(labels3.map(_.name).map(LynxNodeLabel), props3.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
          direction, upperLimit, lowerLimit).map(
          triple =>
            Seq(triple.startNode, triple.storedRelation, triple.endNode)
        )
      }

    )
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

case class PPTAggregation(aggregations: Seq[ReturnItem], groupings: Seq[ReturnItem])(implicit val in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = Seq(in)

  override def withChildren(children0: Seq[PPTNode]): PPTAggregation = PPTAggregation(aggregations, groupings)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = (groupings ++ aggregations).map(x => x.name -> x.expression).map { col =>
    col._1 -> typeOf(col._2, in.schema.toMap)
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.groupBy(groupings.map(x => x.name -> x.expression), aggregations.map(x => x.name -> x.expression))(ctx.expressionContext)
  }
}

case class PPTProcedureCall(procedureNamespace: Namespace, procedureName: ProcedureName, declaredArguments: Option[Seq[Expression]])(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override def withChildren(children0: Seq[PPTNode]): PPTProcedureCall = PPTProcedureCall(procedureNamespace, procedureName, declaredArguments)(plannerContext)

  override val schema: Seq[(String, LynxType)] = {
    val Namespace(parts: List[String]) = procedureNamespace
    val ProcedureName(name: String) = procedureName

    procedureRegistry.getProcedure(parts, name, declaredArguments.map(_.size).getOrElse(0)) match {
      case Some(procedure) =>
        procedure.outputs
      case None => throw UnknownProcedureException(parts, name)
    }
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val Namespace(parts: List[String]) = procedureNamespace
    val ProcedureName(name: String) = procedureName

    procedureRegistry.getProcedure(parts, name, declaredArguments.map(_.size).getOrElse(0)) match {
      case Some(procedure) =>
        val args = declaredArguments match {
          case Some(args) => args.map(eval(_)(ctx.expressionContext))
          case None => procedure.inputs.map(arg => ctx.expressionContext.params.getOrElse(arg._1, LynxNull))
        }

        //        procedure.checkArguments((parts :+ name).mkString("."), args)
        DataFrame(procedure.outputs, () => Iterator(Seq(procedure.call(args))))

      case None => throw UnknownProcedureException(parts, name)
    }
  }
}

case class PPTCreateIndex(labelName: LabelName, properties: List[PropertyKeyName])(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    graphModel._helper.createIndex(labelName.name, properties.map(_.name).toSet)
    DataFrame.empty
  }

  override def withChildren(children0: Seq[PPTNode]): PPTNode = this

  override val schema: Seq[(String, LynxType)] = {
    Seq("CreateIndex" -> CTAny)
  }
}

///////////////////////merge/////////////
trait MergeElement {
  def getSchema: String
}

case class MergeNode(varName: String, labels: Seq[LabelName], properties: Option[Expression]) extends MergeElement {
  override def getSchema: String = varName
}

case class MergeRelationship(varName: String, types: Seq[RelTypeName], properties: Option[Expression], varNameLeftNode: String, varNameRightNode: String, direction: SemanticDirection) extends MergeElement {
  override def getSchema: String = varName
}

case class PPTMergeAction(actions: Seq[MergeAction])(implicit in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override def withChildren(children0: Seq[PPTNode]): PPTMergeAction = PPTMergeAction(actions)(children0.head, plannerContext)

  override val children: Seq[PPTNode] = Seq(in)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    PPTSetClause(Seq.empty, actions)(in, plannerContext).execute(ctx)
  }
}


case class PPTMergeTranslator(m: Merge) extends PPTNodeTranslator {
  def translate(in: Option[PPTNode])(implicit plannerContext: PhysicalPlannerContext): PPTNode = {
    val definedVars = in.map(_.schema.map(_._1)).getOrElse(Seq.empty).toSet
    val mergeOps = mutable.ArrayBuffer[MergeElement]()
    val mergeSchema = mutable.ArrayBuffer[(String, LynxType)]()

    m.pattern.patternParts.foreach {
      case EveryPath(element) => {
        element match {
          case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) => {
            val leftNodeName = var1.map(_.name).getOrElse(s"__NODE_${element.hashCode}")
            mergeSchema.append((leftNodeName, CTNode))
            mergeOps.append(MergeNode(leftNodeName, labels1, properties1))
          }
          case chain: RelationshipChain => {
            buildMerge(chain, definedVars, mergeSchema, mergeOps)
          }
        }
      }
    }

    PPTMerge(mergeSchema, mergeOps)(in, plannerContext)
  }

  private def buildMerge(chain: RelationshipChain, definedVars: Set[String], mergeSchema: mutable.ArrayBuffer[(String, LynxType)], mergeOps: mutable.ArrayBuffer[MergeElement]): String = {
    val RelationshipChain(
    left,
    rp@RelationshipPattern(var2: Option[LogicalVariable], types: Seq[RelTypeName], length: Option[Option[Range]], properties2: Option[Expression], direction: SemanticDirection, legacyTypeSeparator: Boolean, baseRel: Option[LogicalVariable]),
    rnp@NodePattern(var3, labels3: Seq[LabelName], properties3: Option[Expression], _)
    ) = chain

    val varRelation = var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rp.hashCode}")
    val varRightNode = var3.map(_.name).getOrElse(s"__NODE_${rnp.hashCode}")
    left match {
      //create (m)-[r]-(n), left=n
      case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) =>
        val varLeftNode = var1.map(_.name).getOrElse(s"__NODE_${left.hashCode}")
        mergeSchema.append((varLeftNode, CTNode))
        mergeSchema.append((varRelation, CTRelationship))
        mergeSchema.append((varRightNode, CTNode))

        mergeOps.append(MergeNode(varLeftNode, labels1, properties1))
        mergeOps.append(MergeRelationship(varRelation, types, properties2, varLeftNode, varRightNode, direction))
        mergeOps.append(MergeNode(varRightNode, labels3, properties3))

        varRightNode

      // (m)-[p]-(t)-[r]-(n), leftChain=(m)-[p]-(t)
      case leftChain: RelationshipChain =>
        // (m)-[p]-(t)
        val lastNode = buildMerge(leftChain, definedVars, mergeSchema, mergeOps)
        mergeSchema.append((varRelation, CTRelationship))
        mergeSchema.append((varRightNode, CTNode))

        mergeOps.append(MergeRelationship(varRelation, types, properties2, lastNode, varRightNode, direction))
        mergeOps.append(MergeNode(varRightNode, labels3, properties3))

        varRightNode
    }

  }
}

// if pattern not exists, create all elements in mergeOps
case class PPTMerge(mergeSchema: Seq[(String, LynxType)], mergeOps: Seq[MergeElement])(implicit val in: Option[PPTNode], val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = in.toSeq

  override def withChildren(children0: Seq[PPTNode]): PPTMerge = PPTMerge(mergeSchema, mergeOps)(children0.headOption, plannerContext)

  override val schema: Seq[(String, LynxType)] = mergeSchema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext

//    def createNode(nodesToCreate: Seq[MergeNode],
//                   mergedNodesAndRels: mutable.Map[String, Seq[LynxValue]],
//                   ctxMap: Map[String, LynxValue] = Map.empty,
//                   forceToCreate: Boolean = false): Unit = {
//      nodesToCreate.foreach(mergeNode => {
//        val MergeNode(varName, labels, properties) = mergeNode
//        val nodeFilter = NodeFilter(labels.map(f => f.name), properties.map {
//          case MapExpression(items) =>
//            items.map({
//              case (k, v) => k.name -> eval(v)(ec.withVars(ctxMap))
//            })
//        }.getOrElse(Seq.empty).toMap)
//
//        val res = {
//          if (ctxMap.contains(varName)) ctxMap(varName)
//          else graphModel.mergeNode(nodeFilter, forceToCreate, ctx.tx)
//        }
//
//        mergedNodesAndRels += varName -> Seq(res)
//      })
//    }
//
//    def createRelationship(relsToCreate: Seq[MergeRelationship], createdNodesAndRels: mutable.Map[String, Seq[LynxValue]], ctxMap: Map[String, LynxValue] = Map.empty, forceToCreate: Boolean = false): Unit = {
//      relsToCreate.foreach(mergeRels => {
//        val MergeRelationship(varName, types, properties, varNameLeftNode, varNameRightNode, direction) = mergeRels
//        val relationshipFilter = RelationshipFilter(types.map(t => t.name),
//          properties.map {
//            case MapExpression(items) =>
//              items.map({
//                case (k, v) => k.name -> eval(v)(ec.withVars(ctxMap))
//              })
//          }.getOrElse(Seq.empty[(String, LynxValue)]).toMap)
//
//        val leftNode = {
//          if (createdNodesAndRels.contains(varNameLeftNode)) createdNodesAndRels(varNameLeftNode).head
//          else if (ctxMap.contains(varNameLeftNode)) ctxMap(varNameLeftNode)
//          else ???
//        }
//        val rightNode = {
//          if (createdNodesAndRels.contains(varNameRightNode)) createdNodesAndRels(varNameRightNode).head
//          else if (ctxMap.contains(varNameRightNode)) ctxMap(varNameRightNode)
//          else ???
//        }
//
//        val res = graphModel.mergeRelationship(relationshipFilter, leftNode.asInstanceOf[LynxNode], rightNode.asInstanceOf[LynxNode], direction, forceToCreate, ctx.tx)
//
//        createdNodesAndRels += varName -> Seq(res.storedRelation)
//      })
//    }

    children match {
      case Seq(pj@PPTJoin(filterExpr, isSingleMatch, bigTableIndex)) => {
        val searchVar = pj.children.head.schema.toMap
        val res = children.map(_.execute).head.records
        if (res.nonEmpty) {
          plannerContext.pptContext ++= Map("MergeAction" -> false)
          DataFrame(pj.schema, () => res)
        }
        else {
          plannerContext.pptContext ++= Map("MergeAction" -> true)

          val toCreateSchema = mergeSchema.filter(f => !searchVar.contains(f._1))
          val toCreateList = mergeOps.filter(f => !searchVar.contains(f.getSchema))
          val nodesToCreate = toCreateList.filter(f => f.isInstanceOf[MergeNode]).map(f => f.asInstanceOf[MergeNode])
          val relsToCreate = toCreateList.filter(f => f.isInstanceOf[MergeRelationship]).map(f => f.asInstanceOf[MergeRelationship])

          val dependDf = pj.children.head.execute
          val anotherDf = pj.children.last

          val func = dependDf.records.map {
            record => {
              val ctxMap = dependDf.schema.zip(record).map(x => x._1._1 -> x._2).toMap

              val mergedNodesAndRels = mutable.Map[String, Seq[LynxValue]]()

              val forceToCreate = {
                if (anotherDf.isInstanceOf[PPTExpandPath] || anotherDf.isInstanceOf[PPTRelationshipScan]) true
                else false
              }

//              createNode(nodesToCreate, mergedNodesAndRels, ctxMap, forceToCreate)
//              createRelationship(relsToCreate, mergedNodesAndRels, ctxMap, forceToCreate)

              record ++ toCreateSchema.flatMap(f => mergedNodesAndRels(f._1))
            }
          }
          DataFrame(dependDf.schema ++ toCreateSchema, () => func)
        }
      }
      case _ => {
        // only merge situation
        val res = children.map(_.execute).head.records
        if (res.nonEmpty) {
          plannerContext.pptContext ++= Map("MergeAction" -> false)
          DataFrame(mergeSchema, () => res)
        }
        else {
          plannerContext.pptContext ++= Map("MergeAction" -> true)
          val createdNodesAndRels = mutable.Map[String, Seq[LynxValue]]()
          val nodesToCreate = mergeOps.filter(f => f.isInstanceOf[MergeNode]).map(f => f.asInstanceOf[MergeNode])
          val relsToCreate = mergeOps.filter(f => f.isInstanceOf[MergeRelationship]).map(f => f.asInstanceOf[MergeRelationship])

//          createNode(nodesToCreate, createdNodesAndRels, Map.empty, true)
//          createRelationship(relsToCreate, createdNodesAndRels, Map.empty, true)

          val res = Seq(mergeSchema.flatMap(f => createdNodesAndRels(f._1))).toIterator
          DataFrame(mergeSchema, () => res)
        }
      }
    }
  }
}

/////////////////////////////////////////

trait CreateElement

case class CreateNode(varName: String, labels3: Seq[LabelName], properties3: Option[Expression]) extends CreateElement

case class CreateRelationship(varName: String, types: Seq[RelTypeName], properties: Option[Expression], varNameLeftNode: String, varNameRightNode: String) extends CreateElement

case class PPTCreateTranslator(c: Create) extends PPTNodeTranslator {
  def translate(in: Option[PPTNode])(implicit plannerContext: PhysicalPlannerContext): PPTNode = {
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

    PPTCreate(schemaLocal, ops)(in, plannerContext)
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

case class PPTCreate(schemaLocal: Seq[(String, LynxType)], ops: Seq[CreateElement])(implicit val in: Option[PPTNode], val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = in.toSeq

  override def withChildren(children0: Seq[PPTNode]): PPTCreate = PPTCreate(schemaLocal, ops)(children0.headOption, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.map(_.schema).getOrElse(Seq.empty) ++ schemaLocal

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext
    val df = in.map(_.execute(ctx)).getOrElse(createUnitDataFrame(Seq.empty))
    //DataFrame should be generated first
    DataFrame.cached(schema, df.records.map {
      record =>
        val ctxMap = df.schema.zip(record).map(x => x._1._1 -> x._2).toMap
        val nodesInput = ArrayBuffer[(String, NodeInput)]()
        val relsInput = ArrayBuffer[(String, RelationshipInput)]()

        ops.foreach(_ match {
          case CreateNode(varName: String, labels: Seq[LabelName], properties: Option[Expression]) =>
            if (!ctxMap.contains(varName) && nodesInput.find(_._1 == varName).isEmpty) {
              nodesInput += varName -> NodeInput(labels.map(_.name).map(LynxNodeLabel), properties.map {
                case MapExpression(items) =>
                  items.map({
                    case (k, v) => LynxPropertyKey(k.name) -> eval(v)(ec.withVars(ctxMap))
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

            relsInput += varName -> RelationshipInput(types.map(_.name).map(LynxRelationshipType), properties.map {
              case MapExpression(items) =>
                items.map({
                  case (k, v) => LynxPropertyKey(k.name) -> eval(v)(ec.withVars(ctxMap))
                })
            }.getOrElse(Seq.empty[(LynxPropertyKey, LynxValue)]), nodeInputRef(varNameLeftNode), nodeInputRef(varNameRightNode))
        })

        record ++ graphModel.createElements(
          nodesInput,
          relsInput,
          (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
            val created = nodesCreated.toMap ++ relsCreated
            schemaLocal.map(x => created(x._1))
          })
    }.toSeq)
  }
}

case class PPTDelete(delete: Delete)(implicit val in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = Seq(in)

  override def withChildren(children0: Seq[PPTNode]): PPTDelete = PPTDelete(delete)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = Seq.empty

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val schema1: Map[String, (CypherType, Int)] = df.schema.zipWithIndex.map(x => x._1._1 -> (x._1._2, x._2)).toMap
    delete.expressions.map(expr => {
      val colSchema = schema1(expr.asInstanceOf[Variable].name)
      colSchema._1 match {
        case CTRelationship => {
          val relIDs = df.records.map(row => {
            val toBeDeleted = row.apply(colSchema._2)
            toBeDeleted.asInstanceOf[LynxRelationship].id
          })
          graphModel.deleteRelations(relIDs)
        }
        case CTNode => {
          val nodeIDs = df.records.map(row => {
            val toBeDeleted = row.apply(colSchema._2)
            toBeDeleted.asInstanceOf[LynxNode].id
          })
          graphModel.deleteNodesSafely(nodeIDs, delete.forced)
        }
      }
    })
    DataFrame.empty
  }
}

//////// SET ///////
case class PPTSetClauseTranslator(setItems: Seq[SetItem]) extends PPTNodeTranslator {
  override def translate(in: Option[PPTNode])(implicit ppc: PhysicalPlannerContext): PPTNode = {
    PPTSetClause(setItems)(in.get, ppc)
  }
}

case class PPTSetClause(var setItems: Seq[SetItem], mergeAction: Seq[MergeAction] = Seq.empty)(implicit val in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {

  override val children: Seq[PPTNode] = Seq(in)

  override def withChildren(children0: Seq[PPTNode]): PPTSetClause = PPTSetClause(setItems)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)

    setItems = {
      if (mergeAction.nonEmpty) {
        val isCreate = plannerContext.pptContext("MergeAction").asInstanceOf[Boolean]
        if (isCreate) mergeAction.find(p => p.isInstanceOf[OnCreate]).get.asInstanceOf[OnCreate].action.items
        else mergeAction.find(p => p.isInstanceOf[OnMatch]).get.asInstanceOf[OnMatch].action.items
      }
      else setItems
    }

    // TODO: batch process , not Iterator(one) !!!
    val res = df.records.map(n => {
      val ctxMap = df.schema.zip(n).map(x => x._1._1 -> x._2).toMap

      n.size match {
        // set node
        case 1 => {
          var tmpNode = n.head.asInstanceOf[LynxNode]
          setItems.foreach {
            case sp@SetPropertyItem(property, literalExpr) => {
              val Property(map, keyName) = property
              map match {
                case v@Variable(name) => {
                  val data = Array(keyName.name -> eval(literalExpr)(ctx.expressionContext.withVars(ctxMap)).value)
                  tmpNode = graphModel.setNodesProperties(Iterator(tmpNode.id), data, false).next().get
                }
                case cp@CaseExpression(expression, alternatives, default) => {
                  val res = eval(cp)(ctx.expressionContext.withVars(ctxMap))
                  res match {
                    case LynxNull => tmpNode = n.head.asInstanceOf[LynxNode]
                    case _ => {
                      val data = Array(keyName.name -> eval(literalExpr)(ctx.expressionContext.withVars(ctxMap)).value)
                      tmpNode = graphModel.setNodesProperties(Iterator(res.asInstanceOf[LynxNode].id), data, false).next().get
                    }
                  }
                }
              }
            }
            case sl@SetLabelItem(variable, labels) => {
              tmpNode = graphModel.setNodesLabels(Iterator(tmpNode.id), labels.map(f => f.name).toArray).next().get
            }
            case si@SetIncludingPropertiesFromMapItem(variable, expression) => {
              expression match {
                case MapExpression(items) => {
                  val data = items.map(f => f._1.name -> eval(f._2)(ctx.expressionContext.withVars(ctxMap)).value)
                  tmpNode = graphModel.setNodesProperties(Iterator(tmpNode.id), data.toArray, false).next().get
                }
              }
            }
            case sep@SetExactPropertiesFromMapItem(variable, expression) => {
              expression match {
                case MapExpression(items) => {
                  val data = items.map(f => f._1.name -> eval(f._2)(ctx.expressionContext.withVars(ctxMap)).value)
                  tmpNode = graphModel.setNodesProperties(Iterator(tmpNode.id), data.toArray, true).next().get
                }
              }
            }
          }
          Seq(tmpNode)
        }
        // set join TODO copyNode
        case 2 => {
          var tmpNode = Seq[LynxValue]()
          setItems.foreach {
            case sep@SetExactPropertiesFromMapItem(variable, expression) => {
              expression match {
                case v@Variable(name) => {
                  val srcNode = ctxMap(variable.name).asInstanceOf[LynxNode]
                  val maskNode = ctxMap(name).asInstanceOf[LynxNode]
//                  tmpNode = graphModel.copyNode(srcNode, maskNode, ctx.tx)
                }
              }
            }
          }
          tmpNode
        }
        // set relationship
        case 3 => {
          val triple = n
          setItems.foreach {
            case sp@SetPropertyItem(property, literalExpr) => {
              val Property(variable, keyName) = property
              val data = Array(keyName.name -> eval(literalExpr)(ctx.expressionContext.withVars(ctxMap)).value)
              val newRel = graphModel.setRelationshipsProperties(Iterator(triple(1).asInstanceOf[LynxRelationship].id), data).next().get
              Seq(triple.head, newRel, triple.last)
            }
            case sl@SetLabelItem(variable, labels) => {
              // TODO: An relation is able to have multi-type ???
              val newRel = graphModel.setRelationshipsType(Iterator(triple(1).asInstanceOf[LynxRelationship].id), labels.map(f => f.name).toArray.head)
              Seq(triple.head, newRel, triple.last)
            }
            case si@SetIncludingPropertiesFromMapItem(variable, expression) => {
              expression match {
                case MapExpression(items) => {
                  items.foreach(f => {
                    val data = Array(f._1.name -> eval(f._2)(ctx.expressionContext.withVars(ctxMap)).value)
                    val newRel = graphModel.setRelationshipsProperties(Iterator(triple(1).asInstanceOf[LynxRelationship].id), data)
                    Seq(triple.head, newRel, triple.last)
                  })
                }
              }
            }
          }
          triple
        }
      }
    })

    DataFrame.cached(schema, res.toSeq)
  }
}

////////////////////

/////////REMOVE//////////////
case class PPTRemoveTranslator(removeItems: Seq[RemoveItem]) extends PPTNodeTranslator {
  override def translate(in: Option[PPTNode])(implicit ppc: PhysicalPlannerContext): PPTNode = {
    PPTRemove(removeItems)(in.get, ppc)
  }
}

case class PPTRemove(removeItems: Seq[RemoveItem])(implicit val in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {

  override val children: Seq[PPTNode] = Seq(in)

  override def withChildren(children0: Seq[PPTNode]): PPTRemove = PPTRemove(removeItems)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val isWithReturn = ctx.expressionContext.executionContext.statement.returnColumns.nonEmpty
    val res = df.records.map(n => {
      n.size match {
        case 1 => {
          var tmpNode: LynxNode = n.head.asInstanceOf[LynxNode]
          removeItems.foreach {
            case rp@RemovePropertyItem(property) =>
              tmpNode = graphModel.removeNodesProperties(Iterator(tmpNode.id), Array(rp.property.propertyKey.name)).next().get

            case rl@RemoveLabelItem(variable, labels) =>
              tmpNode = graphModel.removeNodesLabels(Iterator(tmpNode.id), rl.labels.map(f => f.name).toArray).next().get
          }
          Seq(tmpNode)
        }
        case 3 => {
          var triple: Seq[LynxValue] = n
          removeItems.foreach {
            case rp@RemovePropertyItem(property) =>
              {
                val newRel = graphModel.removeRelationshipsProperties(Iterator(triple(1).asInstanceOf[LynxRelationship].id), Array(property.propertyKey.name)).next().get
                triple = Seq(triple.head, newRel, triple.last)
              }

            case rl@RemoveLabelItem(variable, labels) =>
              {
                // TODO: An relation is able to have multi-type ???
                val newRel = graphModel.removeRelationshipType(Iterator(triple(1).asInstanceOf[LynxRelationship].id), labels.map(f => f.name).toArray.head).next().get
                triple = Seq(triple.head, newRel, triple.last)
              }
          }
          triple
        }
      }
    })
    if (isWithReturn) DataFrame(schema, () => res)
    else DataFrame.empty
  }
}

////////////////////////////

case class NodeInput(labels: Seq[LynxNodeLabel], props: Seq[(LynxPropertyKey, LynxValue)]) {

}

case class RelationshipInput(types: Seq[LynxRelationshipType], props: Seq[(LynxPropertyKey, LynxValue)], startNodeRef: NodeInputRef, endNodeRef: NodeInputRef) {

}

sealed trait NodeInputRef

case class StoredNodeInputRef(id: LynxId) extends NodeInputRef

case class ContextualNodeInputRef(varname: String) extends NodeInputRef

case class UnresolvableVarException(var0: Option[LogicalVariable]) extends LynxException
