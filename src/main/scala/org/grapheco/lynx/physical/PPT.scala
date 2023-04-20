package org.grapheco.lynx.physical

import org.grapheco.lynx.dataframe.{DataFrame, InnerJoin, JoinType}
import org.grapheco.lynx.evaluator.ExpressionContext
import org.grapheco.lynx.logical.LPTPatternMatch
import org.grapheco.lynx.procedure.{UnknownProcedureException, WrongArgumentException}
import org.grapheco.lynx.runner.{ExecutionContext, GraphModel, NodeFilter, RelationshipFilter}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxCompositeValue, LynxList, LynxMap}
import org.grapheco.lynx.types.property.{LynxBoolean, LynxInteger, LynxNull, LynxNumber, LynxString}
import org.grapheco.lynx.types.spatial.LynxPoint
import org.grapheco.lynx.types.structural._
import org.grapheco.lynx.types.time.LynxTemporalValue
import org.grapheco.lynx.{LynxType, runner}
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions.{Literal, NodePattern, RelationshipChain, _}
import org.opencypher.v9_0.util.InputPosition
import org.opencypher.v9_0.util.symbols.{CTAny, CTList, CTNode, CTPath, CTRelationship}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.{implicitConversions, postfixOps}

object Trans{
  implicit def labelToLynxLabel(l: LabelName): LynxNodeLabel = LynxNodeLabel(l.name)
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

/*
 @param joinType: InnerJoin/FullJoin/LeftJoin/RightJoin
 */
case class PPTJoin(filterExpr: Option[Expression], isSingleMatch: Boolean, joinType: JoinType)(a: PPTNode, b: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = Seq(a, b)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df1 = a.execute(ctx)
    val df2 = b.execute(ctx)

    val df = df1.join(df2, isSingleMatch, joinType)

    if (filterExpr.nonEmpty) {
      val ec = ctx.expressionContext
      val ifNull = joinType match {
        case InnerJoin => false
        case _ => true
      }
      df.filter {
        (record: Seq[LynxValue]) =>
          eval(filterExpr.get)(ec.withVars(df.schema.map(_._1).zip(record).toMap)) match {
            case LynxBoolean(b) => b
            case LynxNull => ifNull
          }
      }(ec)
    }
    else df
  }

  override def withChildren(children0: Seq[PPTNode]): PPTJoin = PPTJoin(filterExpr, isSingleMatch, joinType)(children0.head, children0(1), plannerContext)

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
          case LynxList(l) => l.nonEmpty
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
        record =>
          val path = record.last match {
            case p: LynxPath => p
            case n: LynxNode => LynxPath.startPoint(n)
          }
          graphModel.expand(
            path.endNode.get.id,
            RelationshipFilter(types.map(_.name).map(LynxRelationshipType), properties.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
            NodeFilter(labels2.map(_.name).map(LynxNodeLabel), properties2.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
            direction)
            .map(triple =>
              record ++ Seq(triple.storedRelation, triple.endNode))
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
      graphModel.nodes(
        NodeFilter(
          labels.map(_.name).map(LynxNodeLabel),
          properties.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2)))
            .getOrElse(Map.empty)
        )
      ).map(Seq(_))
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
        var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode,
      )
    }
    else {
      Seq(
        var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
        var2.map(_.name).getOrElse(s"__RELATIONSHIP_LIST_${rel.hashCode}") -> CTList(CTRelationship),
        var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode,
        var2.map(_.name + "LINK").getOrElse(s"__LINK_${rel.hashCode}") -> CTPath
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

    //    length:
    //      [r:XXX] = None
    //      [r:XXX*] = Some(None) // degree 1 to MAX
    //      [r:XXX*..] =Some(Some(Range(None, None))) // degree 1 to MAX
    //      [r:XXX*..3] = Some(Some(Range(None, 3)))
    //      [r:XXX*1..] = Some(Some(Range(1, None)))
    //      [r:XXX*1..3] = Some(Some(Range(1, 3)))
    val (lowerLimit, upperLimit) = length match {
      case None => (1, 1)
      case Some(None) => (1, Int.MaxValue)
      case Some(Some(Range(a, b))) => (a.map(_.value.toInt).getOrElse(1), b.map(_.value.toInt).getOrElse(Int.MaxValue))
    }


    DataFrame(schema,
      () => {
        val paths = graphModel.paths(
          runner.NodeFilter(labels1.map(_.name).map(LynxNodeLabel), props1.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
          runner.RelationshipFilter(types.map(_.name).map(LynxRelationshipType), props2.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
          runner.NodeFilter(labels3.map(_.name).map(LynxNodeLabel), props3.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
          direction, upperLimit, lowerLimit)
        if (length.isEmpty) paths.map { path => Seq(path.startNode.get, path.firstRelationship.get, path.endNode.get) }
        else paths.map { path => Seq(path.startNode.get, path.relationships, path.endNode.get, path) }
//        else paths.map { path => Seq(path.startNode.get, LynxList(path.relationships), path.endNode.get, path.trim) } // fixme: huchuan 2023-04-11: why trim?

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

  val Namespace(parts: List[String]) = procedureNamespace
  val ProcedureName(name: String) = procedureName
  val arguments = declaredArguments.getOrElse(Seq.empty)
  val procedure = procedureRegistry.getProcedure(parts, name, arguments.size)
    .getOrElse {
      throw UnknownProcedureException(parts, name)
    }

  override val schema: Seq[(String, LynxType)] = procedure.outputs

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val args = declaredArguments match {
      case Some(args) => args.map(eval(_)(ctx.expressionContext))
      case None => procedure.inputs.map(arg => ctx.expressionContext.params.getOrElse(arg._1, LynxNull))
    }
    val argsType = args.map(_.lynxType)
    if (procedure.checkArgumentsType(argsType)) {
      DataFrame(procedure.outputs, () => Iterator(Seq(procedure.execute(args))))
    } else {
      throw WrongArgumentException(name, procedure.inputs.map(_._2), argsType)
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


case class PPTDropIndex(labelName: LabelName, properties: List[PropertyKeyName])(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    graphModel._helper.dropIndex(labelName.name, properties.map(_.name).toSet)
    DataFrame.empty
  }

  override def withChildren(children0: Seq[PPTNode]): PPTNode = this

  override val schema: Seq[(String, LynxType)] = {
    Seq("DropIndex" -> CTAny)
  }
}

///////////////////////merge/////////////

case class PPTMergeTranslator(m: Merge) extends PPTNodeTranslator {
  def translate(in: Option[PPTNode])(implicit plannerContext: PhysicalPlannerContext): PPTNode = {
    val definedVars = in.map(_.schema.map(_._1)).getOrElse(Seq.empty).toSet
    val mergeOps = mutable.ArrayBuffer[FormalElement]()
    val mergeSchema = mutable.ArrayBuffer[(String, LynxType)]()

    m.pattern.patternParts.foreach {
      case EveryPath(element) => {
        element match {
          case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) => {
            val leftNodeName = var1.map(_.name).getOrElse(s"__NODE_${element.hashCode}")
            mergeSchema.append((leftNodeName, CTNode))
            mergeOps.append(FormalNode(leftNodeName, labels1, properties1))
          }
          case chain: RelationshipChain => {
            buildMerge(chain, definedVars, mergeSchema, mergeOps)
          }
        }
      }
    }

    PPTMerge(mergeSchema,
      mergeOps,
      m.actions collect { case m: OnMatch => m },
      m.actions collect { case c: OnCreate => c })(in, plannerContext)
  }

  private def buildMerge(chain: RelationshipChain, definedVars: Set[String], mergeSchema: mutable.ArrayBuffer[(String, LynxType)], mergeOps: mutable.ArrayBuffer[FormalElement]): String = {
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

        mergeOps.append(FormalNode(varLeftNode, labels1, properties1))
        mergeOps.append(FormalRelationship(varRelation, types, properties2, varLeftNode, varRightNode)) // direction
        mergeOps.append(FormalNode(varRightNode, labels3, properties3))

        varRightNode

      // (m)-[p]-(t)-[r]-(n), leftChain=(m)-[p]-(t)
      case leftChain: RelationshipChain =>
        // (m)-[p]-(t)
        val lastNode = buildMerge(leftChain, definedVars, mergeSchema, mergeOps)
        mergeSchema.append((varRelation, CTRelationship))
        mergeSchema.append((varRightNode, CTNode))

        mergeOps.append(FormalRelationship(varRelation, types, properties2, lastNode, varRightNode)) // direction
        mergeOps.append(FormalNode(varRightNode, labels3, properties3))

        varRightNode
    }

  }
}

/**
 *
 * @param mergeSchema
 * @param mergeOps
 * @param onMatch
 * @param onCreate
 * @param in
 * @param plannerContext
 */
case class PPTMerge(mergeSchema: Seq[(String, LynxType)],
                    mergeOps: Seq[FormalElement],
                    onMatch: Seq[OnMatch],
                    onCreate: Seq[OnCreate])(implicit val in: Option[PPTNode], val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = in.toSeq

  override def withChildren(children0: Seq[PPTNode]): PPTMerge = PPTMerge(mergeSchema, mergeOps, onMatch, onCreate)(children0.headOption, plannerContext)

  override val schema: Seq[(String, LynxType)] = mergeSchema ++ children.head.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext
    // has match or create?
    var hasMatched = false
    // PPTMerge must has-only-has one child
    val child: PPTNode = children.head
    // Set[(name, CreatedElement)]: aviod to create again
    val distinct: Boolean = !(mergeOps.collectFirst { case f: FormalNode => f }.nonEmpty && mergeOps.collectFirst { case f: FormalRelationship => f }.nonEmpty)

    val createdNode: mutable.HashMap[NodeInput, LynxNode] = if (distinct) mutable.HashMap[NodeInput, LynxNode]() else null

    val df = child match {
      case pj: PPTJoin => {
        val pjRes = pj.execute
        val dropNull = pjRes.records.dropWhile(_.exists(LynxNull.eq))
        if (dropNull.nonEmpty) {
          hasMatched = true
          pjRes.select(schema.map { case (name, _) => (name, None) })
        } else {
          val opsMap = mergeOps.map(ele => ele.varName -> ele).toMap
          val records = pjRes.select(schema.map { case (name, _) => (name, None) }).records.map { record =>
            val recordMap = schema.map(_._1).zip(record).toMap
            val nullCol = recordMap.filter(_._2 == LynxNull).keys.toSeq
            val creation = CreateOps(nullCol.map(opsMap))(e => eval(e)(ec.withVars(recordMap)), graphModel).execute(createdNode).toMap
            record.zip(schema.map(_._1)).map {
              case (LynxNull, name) => creation(name)
              case (v: LynxValue, _) => v
            }
          }.toList
          DataFrame(schema, () => records.toIterator) // danger! so do because the create ops will be do lazy due to inner in the dataframe which is lazy.
        }
      }
      case _ => {
        val res = child.execute.records
        if (res.nonEmpty) {
          hasMatched = true
          DataFrame(schema, () => res)
        } else {
          val creation = CreateOps(mergeOps)(e => eval(e)(ec), graphModel).execute()
          DataFrame(schema, () => Iterator(schema.map(x => creation.toMap.getOrElse(x._1, LynxNull))))
        }
      }
    }
    // actions
    val items = if (hasMatched) onMatch.flatMap(_.action.items) else onCreate.flatMap(_.action.items)
    if (items.nonEmpty) {
      PPTSetClause(items)(new PPTNode { // temp PPTNode to execute SetClause
        override val schema: Seq[(String, LynxType)] = df.schema

        override def execute(implicit ctx: ExecutionContext): DataFrame = df

        override def withChildren(children0: Seq[PPTNode]): PPTNode = ???
      }, plannerContext).execute(ctx)
    } else df
  }
}
/////////////////////////////////////////

trait FormalElement {
  def varName: String
}

case class FormalNode(varName: String, labels: Seq[LabelName], properties: Option[Expression]) extends FormalElement

case class FormalRelationship(varName: String, types: Seq[RelTypeName], properties: Option[Expression], varNameLeftNode: String, varNameRightNode: String) extends FormalElement

case class CreateOps(ops: Seq[FormalElement])(eval: Expression => LynxValue, graphModel: GraphModel) {
  def execute(distinct: mutable.Map[NodeInput, LynxNode] = null): Seq[(String, LynxValue with LynxElement)] = {
    val nodesInput = ArrayBuffer[(String, NodeInput)]()
    val relsInput = ArrayBuffer[(String, RelationshipInput)]()

    ops.foreach {
      case FormalNode(varName: String, labels: Seq[LabelName], properties: Option[Expression]) =>
        nodesInput += varName -> NodeInput(labels.map(_.name).map(LynxNodeLabel), properties match {
          case Some(MapExpression(items)) => items.map { case (k, v) => LynxPropertyKey(k.name) -> eval(v) }
          case None => Seq.empty
        })

      case FormalRelationship(varName: String, types: Seq[RelTypeName], properties: Option[Expression], varNameLeftNode: String, varNameRightNode: String) =>

        def nodeInputRef(nodeVarName: String): NodeInputRef = eval(Variable(nodeVarName)(InputPosition.NONE)) match {
          case node: LynxNode => StoredNodeInputRef(node.id)
          case _ => ContextualNodeInputRef(nodeVarName)
        }

        relsInput += varName -> RelationshipInput(types.map(_.name).map(LynxRelationshipType), properties match {
          case Some(MapExpression(items)) => items.map { case (k, v) => LynxPropertyKey(k.name) -> eval(v) }
          case None => Seq.empty
        }, nodeInputRef(varNameLeftNode), nodeInputRef(varNameRightNode))
    }

    if (distinct == null) graphModel.createElements(nodesInput, relsInput, _ ++ _)
    else {
      val existNode = nodesInput.filter(sn => distinct.contains(sn._2))
      val notExistNode = nodesInput.filterNot(sn => distinct.contains(sn._2))
      def nodeRef(node: NodeInputRef): NodeInputRef = node match {
        case ContextualNodeInputRef(name) => existNode.find(_._1 == name).map(_._2).map(distinct).map(_.id).map(StoredNodeInputRef).getOrElse(ContextualNodeInputRef(name))
        case o => o
      }
      val relsInputWithNode = relsInput.map{ case(str,relInput) =>
        (str, RelationshipInput(relInput.types, relInput.props, nodeRef(relInput.startNodeRef), nodeRef(relInput.endNodeRef)))
      }
      val newCreated = graphModel.createElements(notExistNode, relsInputWithNode, _ ++ _)
      val _map = newCreated.toMap
      distinct ++= notExistNode.map{ case (str, input) => (input, _map(str).asInstanceOf[LynxNode])}
      existNode.map{case(str, inp) => (str,distinct(inp))} ++ newCreated
    }
  }
}

case class PPTCreateTranslator(c: Create) extends PPTNodeTranslator {
  def translate(in: Option[PPTNode])(implicit plannerContext: PhysicalPlannerContext): PPTNode = {
    val definedVars = in.map(_.schema.map(_._1)).getOrElse(Seq.empty).toSet
    val (schemaLocal, ops) = c.pattern.patternParts.foldLeft((Seq.empty[(String, LynxType)], Seq.empty[FormalElement])) {
      (result, part) =>
        val (schema1, ops1) = result
        part match {
          case EveryPath(element) =>
            element match {
              //create (n)
              case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) =>
                val leftNodeName = var1.map(_.name).getOrElse(s"__NODE_${element.hashCode}")
                (schema1 :+ (leftNodeName -> CTNode)) ->
                  (ops1 :+ FormalNode(leftNodeName, labels1, properties1))

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
  private def build(chain: RelationshipChain, definedVars: Set[String]): (String, Seq[(String, LynxType)], Seq[FormalElement]) = {
    val RelationshipChain(
    left,
    rp@RelationshipPattern(var2: Option[LogicalVariable], types: Seq[RelTypeName], length: Option[Option[Range]], properties2: Option[Expression], direction: SemanticDirection, legacyTypeSeparator: Boolean, baseRel: Option[LogicalVariable]),
    rnp@NodePattern(var3, labels3: Seq[LabelName], properties3: Option[Expression], _)
    ) = chain

    val varRelation = var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rp.hashCode}")
    val varRightNode = var3.map(_.name).getOrElse(s"__NODE_${rnp.hashCode}")

    val schemaLocal = ArrayBuffer[(String, LynxType)]()
    val opsLocal = ArrayBuffer[FormalElement]()
    left match {
      //create (m)-[r]-(n), left=n
      case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) =>
        val varLeftNode = var1.map(_.name).getOrElse(s"__NODE_${left.hashCode}")
        if (!definedVars.contains(varLeftNode)) {
          schemaLocal += varLeftNode -> CTNode
          opsLocal += FormalNode(varLeftNode, labels1, properties1)
        }

        if (!definedVars.contains(varRightNode)) {
          schemaLocal += varRightNode -> CTNode
          opsLocal += FormalNode(varRightNode, labels3, properties3)
        }

        schemaLocal += varRelation -> CTRelationship
        opsLocal += FormalRelationship(varRelation, types, properties2, varLeftNode, varRightNode)

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
            Seq(FormalNode(varRightNode, labels3, properties3))
          } else {
            Seq.empty
          }) ++ Seq(
            FormalRelationship(varRelation, types, properties2, varLastNode, varRightNode)
          )
        )
    }
  }
}

case class PPTCreate(schemaLocal: Seq[(String, LynxType)], ops: Seq[FormalElement])(implicit val in: Option[PPTNode], val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
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

        ops.foreach {
          case FormalNode(varName: String, labels: Seq[LabelName], properties: Option[Expression]) =>
            if (!ctxMap.contains(varName) && !nodesInput.exists(_._1 == varName)) {
              nodesInput += varName ->
                NodeInput(labels.map(Trans.labelToLynxLabel),
                  properties.map(eval(_)(ec.withVars(ctxMap))).map {
                    case LynxMap(m) => m.map{ case (str, value) => LynxPropertyKey(str) -> value}.toSeq
                    case _ => throw SyntaxErrorException("Property should be a Map.")
                  }.getOrElse(Seq.empty))
            }

          case FormalRelationship(varName: String, types: Seq[RelTypeName], properties: Option[Expression], varNameLeftNode: String, varNameRightNode: String) =>

            def nodeInputRef(varname: String): NodeInputRef = {
              ctxMap.get(varname).map(
                x =>
                  StoredNodeInputRef(x.asInstanceOf[LynxNode].id)
              ).getOrElse(
                ContextualNodeInputRef(varname)
              )
            }

            relsInput += varName ->
              RelationshipInput(types.map(_.name).map(LynxRelationshipType),
                properties.map(eval(_)(ec.withVars(ctxMap))).map {
                  case LynxMap(m) => m.map { case (str, value) => LynxPropertyKey(str) -> value }.toSeq
                  case _ => throw SyntaxErrorException("Property should be a Map.")
                }.getOrElse(Seq.empty), nodeInputRef(varNameLeftNode), nodeInputRef(varNameRightNode))
        }

        record ++ graphModel.createElements(
          nodesInput,
          relsInput,
          (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
            graphModel.write.commit
            val created = nodesCreated.toMap ++ relsCreated
            schemaLocal.map(x => created(x._1))
          })
    }.toSeq)
  }
}

/**
 * The DELETE clause is used to delete graph elements — nodes, relationships or paths.
 *
 * @param delete
 * @param in
 * @param plannerContext
 */
case class PPTDelete(delete: Delete)(implicit val in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = Seq(in)

  override def withChildren(children0: Seq[PPTNode]): PPTDelete = PPTDelete(delete)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = Seq.empty

  override def execute(implicit ctx: ExecutionContext): DataFrame = { // TODO so many bugs !
    val df = in.execute(ctx)
    delete.expressions foreach { exp =>
      val projected = df.project(Seq(("delete", exp)))(ctx.expressionContext)
      val (_, elementType) = projected.schema.head
      elementType match {
        case CTNode => graphModel.deleteNodesSafely(
          dropNull(projected.records) map {
            _.asInstanceOf[LynxNode].id
          }, delete.forced)
        case CTRelationship => graphModel.deleteRelations(
          dropNull(projected.records) map {
            _.asInstanceOf[LynxRelationship].id
          })
        case CTPath =>
        case _ => throw SyntaxErrorException(s"expected Node, Path pr Relationship, but a ${elementType}")
      }
    }

    def dropNull(values: Iterator[Seq[LynxValue]]): Iterator[LynxValue] =
      values.flatMap(_.headOption.filterNot(LynxNull.equals))

    DataFrame.empty
  }


}

//////// SET ///////
case class PPTSetClauseTranslator(setItems: Seq[SetItem]) extends PPTNodeTranslator {
  override def translate(in: Option[PPTNode])(implicit ppc: PhysicalPlannerContext): PPTNode = {
    PPTSetClause(setItems)(in.get, ppc)
  }
}

case class PPTSetClause(setItems: Seq[SetItem])(implicit val in: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode with WritePlan {

  override val children: Seq[PPTNode] = Seq(in)

  override def withChildren(children0: Seq[PPTNode]): PPTSetClause = PPTSetClause(setItems)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val records = df.records.toList // danger!
    val columnsName = df.columnsName
    /*
    setItems case:
      - SetLabelItem(Variable, Seq[LabelName])
      - SetPropertyItem(LogicalProperty, Expression)
        case expr
      - SetExactPropertiesFromMapItem(Variable, Expression) p={...}
      - SetIncludingPropertiesFromMapItem(Variable, Expression) p+={...}
     */
    def getIndex(name: String): Int = columnsName.indexOf(name) // throw

    trait SetOp

    case class CaseSet(caseExp: CaseExpression, propOp: (Map[LynxPropertyKey, LynxValue], Expression => LynxValue)=> Map[LynxPropertyKey, LynxValue]) extends SetOp

    case class Label(labelOp: Seq[LynxNodeLabel] => Seq[LynxNodeLabel]) extends SetOp

    case class Static(propOp: Map[LynxPropertyKey, LynxValue] => Map[LynxPropertyKey, LynxValue]) extends SetOp

    case class Dynamic(propOp: (Map[LynxPropertyKey, LynxValue], Expression => LynxValue)=> Map[LynxPropertyKey, LynxValue]) extends SetOp

    // START process for map item
    def fromMapItem(name: String, expression: Expression, including: Boolean): (Int, SetOp) = (
      getIndex(name),
      expression match {
        case l: Literal=> {
          val theMap = toMap(eval(expression)(ctx.expressionContext))
          if (including) Static(_ ++ theMap)
          else Static(_ => theMap)
        }
        case p: Parameter => {
          val theMap = toMap(eval(expression)(ctx.expressionContext))
          if (including) Static(_ ++ theMap)
          else Static(_ => theMap)
        }
        case _ => if (including) Dynamic((old, evalD)=> old ++ toMap(evalD(expression)))
          else Dynamic((_, evalD)=> toMap(evalD(expression)))
      }
    )

    def toMap(v: LynxValue): Map[LynxPropertyKey, LynxValue] = v match {
      case m: LynxMap => m .v.map{ case (str, value) => LynxPropertyKey(str) -> value}
      case h: HasProperty => h.keys.map(k => k->h.property(k).getOrElse(LynxNull)).toMap
      case o => throw ExecuteException(s"can't find props map from type ${o.lynxType}")
    }
    //END
    /*
      Map[columnIndex, Seq[Ops]]
     */
    val ops: Map[Int, Seq[SetOp]] = setItems.map {
      case SetLabelItem(Variable(name), labels) => (getIndex(name), Label(_ ++ labels.map(_.name).map(LynxNodeLabel)))
      case SetPropertyItem(LogicalProperty(key, map), expression) =>
        (key,expression) match {
          case (Variable(name), l: Literal) => (getIndex(name), {
            val newData = eval(expression)(ctx.expressionContext)
            Static(old => old + (LynxPropertyKey(map.name) -> newData))
          })
          case (Variable(name), p: Parameter) => (getIndex(name), {
            val newData = eval(expression)(ctx.expressionContext)
            Static(old => old + (LynxPropertyKey(map.name) -> newData))
          })
          case (Variable(name), _) => (getIndex(name), Dynamic((old, evalD) => old + (LynxPropertyKey(map.name) -> evalD(expression))))
          case (ce: CaseExpression, l: Literal) => (getIndex(ce.alternatives.head._2.asInstanceOf[Variable].name),{
            val newData = eval(expression)(ctx.expressionContext)
            Dynamic((old, evalD) => evalD(ce) match {
              case LynxNull => old
              case _ => old + (LynxPropertyKey(map.name) -> newData)
            })
          })
          case (ce: CaseExpression, _) => (getIndex(ce.alternatives.head._2.asInstanceOf[Variable].name),
            Dynamic((old, evalD) => evalD(ce) match {
              case LynxNull => old
              case _ => old + (LynxPropertyKey(map.name) -> evalD(expression))
            }))
          case _ => throw ExecuteException("Unsupported type of logical property")
        }
      case SetExactPropertiesFromMapItem(Variable(name), expression) => fromMapItem(name, expression, false)
      case SetIncludingPropertiesFromMapItem(Variable(name), expression) => fromMapItem(name, expression, true)
    }.groupBy(_._1).mapValues(_.map(_._2)) //group ops of same column

    val needIndexes = ops.keySet

    DataFrame(schema, () => records.map{ record =>
      record.zipWithIndex.map{
        case (e:LynxElement, index) if needIndexes.contains(index) => {
          val opsOfIt = ops(index)
          // setLabels if e is Node
          val updatedLabels = e match {
            case n: LynxNode => opsOfIt.collect { case Label(labelOp) => labelOp }
              .foldLeft(n.labels){(labels, ops)=>ops(labels)} // (default Label) => (op1) => (op2) => ... => (updated)
            case _ => Seq.empty
          }

          val updatedProps = opsOfIt.filterNot(_.isInstanceOf[Label])
            .foldLeft(e.keys.map(k => k->e.property(k).getOrElse(LynxNull)).toMap){
              (props, ops) => ops match {
                case Static(propOp) => propOp(props)
                case Dynamic(propOp) => propOp(props, eval(_)(ctx.expressionContext.withVars(columnsName.zip(record).toMap)))
              }
            }

          (e match {
            case n: LynxNode => graphModel.write.updateNode(n.id, updatedLabels, updatedProps)
            case r: LynxRelationship => graphModel.write.updateRelationShip(r.id, updatedProps)
          }).getOrElse(LynxNull)
        }
        case other => other._1 // the column do not need change
      }
    }.toIterator)
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
            case rp@RemovePropertyItem(property) => {
              val newRel = graphModel.removeRelationshipsProperties(Iterator(triple(1).asInstanceOf[LynxRelationship].id), Array(property.propertyKey.name)).next().get
              triple = Seq(triple.head, newRel, triple.last)
            }

            case rl@RemoveLabelItem(variable, labels) => {
              // TODO: An relation is able to have multi-type ???
              val newRel = graphModel.removeRelationshipType(Iterator(triple(1).asInstanceOf[LynxRelationship].id), labels.map(f => f.name).toArray.head).next().get
              triple = Seq(triple.head, newRel, triple.last)
            }
          }
          triple
        }
      }
    })
    DataFrame.cached(schema, res.toSeq)
  }
}

////////////////////////////
case class PPTUnion(distinct: Boolean)(a: PPTNode, b: PPTNode, val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = Seq(a, b)

  override val schema: Seq[(String, LynxType)] = a.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val schema1 = a.schema
    val schema2 = b.schema
    if (!schema1.toSet.equals(schema2.toSet)) throw SyntaxErrorException("All sub queries in an UNION must have the same column names")
    val record1 = a.execute(ctx).records
    val record2 = b.execute(ctx).records
    val df = DataFrame(schema, () => record1 ++ record2)
    if (distinct) df.distinct() else df
  }

  override def withChildren(children0: Seq[PPTNode]): PPTNode = PPTUnion(distinct)(children0.head, children0(1), plannerContext)
}

/////////UNWIND//////////////
case class PPTUnwindTranslator(expression: Expression, variable: Variable) extends PPTNodeTranslator {
  override def translate(in: Option[PPTNode])(implicit ppc: PhysicalPlannerContext): PPTNode = {
    PPTUnwind(expression, variable)(in, ppc)
  }
}

case class PPTUnwind(expression: Expression, variable: Variable)(implicit val in: Option[PPTNode], val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val children: Seq[PPTNode] = in.toSeq

  override val schema: Seq[(String, LynxType)] = in.map(_.schema).getOrElse(Seq.empty) ++ Seq((variable.name, CTAny)) // TODO it is CTAny?

  override def execute(implicit ctx: ExecutionContext): DataFrame = // fixme
    in map { inNode =>
      val df = inNode.execute(ctx) // dataframe of in
      val colName = schema map { case (name, _) => name }
      DataFrame(schema, () => df.records flatMap { record =>
        val recordCtx = ctx.expressionContext.withVars(colName zip (record) toMap)
        val rsl = (expressionEvaluator.eval(expression)(recordCtx) match {
          case list: LynxList => list.value
          case element: LynxValue => List(element)
        }) map { element => record :+ element }
        rsl
      })
    } getOrElse {
      DataFrame(schema, () =>
        eval(expression)(ctx.expressionContext).asInstanceOf[LynxList].value.toIterator map (lv => Seq(lv)))
    }

  override def withChildren(children0: Seq[PPTNode]): PPTUnwind = PPTUnwind(expression, variable)(children0.headOption, plannerContext)
}
////////////////////////////

case class NodeInput(labels: Seq[LynxNodeLabel], props: Seq[(LynxPropertyKey, LynxValue)]) {

}

case class RelationshipInput(types: Seq[LynxRelationshipType], props: Seq[(LynxPropertyKey, LynxValue)], startNodeRef: NodeInputRef, endNodeRef: NodeInputRef) {

}

// This trait is to make sure the write operation is executed, even if there is not a Return Clause.
trait WritePlan {
  def acutalExecute(df: DataFrame): DataFrame = {
    val records: Seq[Seq[LynxValue]] = df.records.toSeq
    DataFrame(df.schema, () => records.iterator)
  }
}


/////////STATISTICS//////////////
case class PPTRelationshipCountFromStatistics(ttype: Option[LynxRelationshipType], variableName: String)(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val schema: Seq[(String, LynxType)] = Seq((variableName, LynxInteger(0).lynxType))

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val stat = plannerContext.runnerContext.graphModel.statistics
    val res = ttype.map(ttype => stat.numRelationshipByType(ttype)).getOrElse(stat.numRelationship)
    DataFrame(schema, () => Iterator(Seq(LynxInteger(res))))
  }

  override def withChildren(children0: Seq[PPTNode]): PPTNode = ???
}

case class PPTNodeCountFromStatistics(label: Option[LynxNodeLabel], variableName: String)(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPPTNode {
  override val schema: Seq[(String, LynxType)] = Seq((variableName, LynxInteger(0).lynxType))

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val stat = plannerContext.runnerContext.graphModel.statistics
    val res = label.map(label => stat.numNodeByLabel(label)).getOrElse(stat.numNode)
    DataFrame(schema, () => Iterator(Seq(LynxInteger(res))))
  }

  override def withChildren(children0: Seq[PPTNode]): PPTNode = ???
}
/////////////////////////////////


