package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxException
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.infer.{Condition, NotMatchInferExecutorFoundException}
import org.grapheco.lynx.logical.plans
import org.grapheco.lynx.logical.plans.{GraphPatternEdge, GraphPatternNode}
import org.grapheco.lynx.physical.planner.cost.Candidate
import org.grapheco.lynx.physical.{NodeInput, PhysicalPlannerContext}
import org.grapheco.lynx.runner.{CONTAINS, EQUAL, ExecutionContext, GREATER_THAN, GREATER_THAN_OR_EQUAL, IN, LESS_THAN, LESS_THAN_OR_EQUAL, NOT_EQUAL, NodeFilter, PropOp, RelationshipFilter}
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.{LTNode, LTVNode, LTVRelationship, LynxType, LynxValue}
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPath, LynxPropertyKey, LynxRelationshipType}
import org.opencypher.v9_0.expressions.{And, Ands, BinaryOperatorExpression, Equals, Expression, HasLabels, In, LabelName, ListLiteral, LogicalVariable, NodePattern, Property, PropertyKeyName, Range, RelTypeName, RelationshipPattern, SemanticDirection, Variable, VirtualNodePattern, VirtualRelationshipPattern}
import org.opencypher.v9_0.util.InputPosition

trait InferPhysicalPlan

object InferPlanner {
  def makeFilters(node: GraphPatternNode)(in: PhysicalPlan)(implicit plannerContext: PhysicalPlannerContext): Seq[PhysicalPlan] = {
    val ip = InputPosition.NONE
    val labelsFilter = node.labels.map(label => HasLabels(Variable(node.variableName)(ip), Seq(label.toNodeLabel))(ip)) match {
      case Seq() => None
      case Seq(expr) => Some(Filter(expr))
      case s:Seq[_] => Filter.multi(s)
    }
    val withLabel = labelsFilter match {
      case None => in
      case Some(filter) => in ~> InferLabel(node.variableName) ~> filter
    }
    if (node.expressions.isEmpty) return Seq(withLabel)
    val exprs = node.expressions.map(extractExpression).reduce(_ ++ _)
    // todo order
    val plan = exprs.foldLeft(withLabel){ case (left, (key, expr)) =>
      left ~> InferProperties(node.variableName, Seq(LynxPropertyKey(key))) ~> Filter(expr)
    }
    Seq(plan)
  }

  def extractExpression(expression: Expression): Map[String, Expression] = expression match {
    case And(left, right) => extractExpression(left) ++ extractExpression(right)
    case Ands(exprs) => exprs.map(extractExpression).reduce(_ ++ _)
    case e@Equals(Property(_,PropertyKeyName(key)), right) => Map(key -> e)
    case i@In(Property(_,PropertyKeyName(key)), right) => Map(key -> i)
    case _ => Map.empty
    // other
  }

  def extractVarProp(expression: Expression): Map[(String, String), Expression] = expression match {
    case And(l, r) => extractVarProp(l) ++ extractVarProp(r)
    case Ands(exprs) => exprs.map(extractVarProp).reduce(_ ++ _)
    case b:BinaryOperatorExpression => extractVarProp(b.lhs) ++ extractVarProp(b.rhs)
    case p@Property(Variable(name), PropertyKeyName(key)) => Map((name, key) -> p)
    case _ => Map.empty
  }

  def addInferToFilter(expression: Expression)(in: PhysicalPlan)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    extractVarProp(expression).map{
      case ((variable, property), _:Property) => InferProperties(variable, Seq(property).map(LynxPropertyKey))
        // case label? TODO
    }.foldLeft(in)(_ ~> _)
  }
}

case class InferPlanner(sourceNode: GraphPatternNode,
                        edge: GraphPatternEdge,
                        targetNode: GraphPatternNode,
                        leftPlan: Candidate,
                        rightPlan: Candidate
                       )(implicit val plannerContext: PhysicalPlannerContext) {

  def plan(): Seq[PhysicalPlan] = if (edge.direction == plans.IN) {
      Seq.empty // infer can not be IN
    } else {
      (sourceNode.virtual, targetNode.virtual) match {
        case (false, true) => planInferExpand()
        case (true, true) => if (rightPlan.plan.isInstanceOf[InferFakeNode]) planInferExpand() else planInferLink()
        case (true, false) => planInferLink()
      }
    }

  //  (A)~[r]~~<B>
  private def planInferExpand(): Seq[PhysicalPlan] =
    InferPlanner.makeFilters(targetNode)(leftPlan.plan ~> InferExpand(sourceNode, edge, targetNode))

  private def planInferLink(): Seq[PhysicalPlan] =
    Seq(InferLink(sourceNode, edge, targetNode).withChildren(Option(leftPlan.plan), Option(rightPlan.plan)))
}

case class VNodeFromList(pattern: GraphPatternNode, listVariable: String)(implicit val plannerContext: PhysicalPlannerContext) extends SinglePhysicalPlan with InferPhysicalPlan{
  override def schema: Seq[(String, LynxType)] = in.schema ++: Seq((pattern.variableName, LTVNode))

  override def execute(implicit ctx: ExecutionContext): DataFrame = profile {
    val df = in.execute(ctx)
    val listIndex = df.columnsName.indexOf(listVariable)
    if (listIndex == -1) DataFrame(schema, () => {Iterator.empty})
    else DataFrame(schema, () => {
      df.records.flatMap{ record =>
        record(listIndex) match {
          case list: LynxList => list.v.map(v => record ++ Seq(v))
          case _ => Iterator.empty
        }
      }
    })
  }
}

case class InferFakeNode(pattern: GraphPatternNode)(implicit val plannerContext: PhysicalPlannerContext) extends LeafPhysicalPlan with InferPhysicalPlan {
  override def schema: Seq[(String, LynxType)] = Seq((pattern.variableName, LTVNode))

  override def execute(implicit ctx: ExecutionContext): DataFrame = DataFrame.empty
}

case class InferExpand(leftNode: GraphPatternNode, rel: GraphPatternEdge, rightNode: GraphPatternNode)(implicit val plannerContext: PhysicalPlannerContext) extends SinglePhysicalPlan with InferPhysicalPlan {

  override def schema: Seq[(String, LynxType)] = in.schema ++ Seq(
      rel.variableName -> LTVRelationship,
      rightNode.variableName -> LTVNode)

  private def leftString = if(leftNode.virtual) s"<${leftNode.variableName}>" else s"(${leftNode.variableName})"
  override def toString: String = s"InferExpand($leftString~[${rel.types.mkString(", ")}]~><${rightNode.variableName}>)"

  override def execute(implicit ctx: ExecutionContext): DataFrame = profile {
    val df = in.execute(ctx)
    val optional = rel.optional
    val inferCondition = Condition(rightNode.labels.map(_.toString), Seq.empty, rel.types.map(_.toString))
    val leftNodeIndex = df.indexOf(leftNode.variableName)
      .getOrElse(throw LynxException("Unknown column name: "+leftNode.variableName))

    DataFrame(schema, () => {
      df.records.flatMap{ record =>
        val endpoint = record(leftNodeIndex) match {
          case p: LynxPath => p.nodes.last
          case n: LynxNode => n
        }
        val inferEngine = ctx.inferEngine
        val inferExecutor = inferEngine.adviser.forExpand(inferCondition)
        if(inferExecutor.isDefined) {
          val expand = inferExecutor.get
          val rsl = expand.infer(endpoint)
          rsl.map { case (relationship, node) =>
//              graphModel.write.createElements(
//                Seq(("", NodeInput(node.labels, node))), Seq(("", RelationshipInput(relationship.relType, relationship.startNodeId, relationship.endNodeId, relationship.props.toSeq))))
              record ++ Seq(relationship, node)}
        } else {
          throw  NotMatchInferExecutorFoundException(inferCondition.toString)
        }
      }
    })
  }
}

case class InferLink(leftNode: GraphPatternNode, rel: GraphPatternEdge, rightNode: GraphPatternNode)
                    (implicit val plannerContext: PhysicalPlannerContext)
  extends DoublePhysicalPlan with InferPhysicalPlan {

  override def toString: String = s"InferLink(<${leftNode.variableName}>?${rel.types.head}?<${rightNode.variableName}>)"

  override def schema: Seq[(String, LynxType)] = l.schema ++ Seq(rel.variableName -> LTVRelationship) ++ r.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = profile {
    val df_l = l.execute(ctx)
    val df_r = r.execute(ctx)

    val optional = rel.optional

    val inferCondition = Condition(leftNode.labels.map(_.toString), Seq.empty, rel.types.map(_.toString), rightNode.labels.map(_.toString))

    val inferEngine = ctx.inferEngine

    val inferExecutor = inferEngine.adviser.forLink(inferCondition).getOrElse(throw  NotMatchInferExecutorFoundException(inferCondition.toString))

    val rightNodesIndex = df_r.columnsName.indexOf(rightNode.variableName)
//    println(df_r.columnsName)
    if (rightNodesIndex == -1) throw LynxException(s"Variable ${rightNode.variableName} not found")
    if (df_r.schema(rightNodesIndex)._2 != LTVNode) throw LynxException(s"Variable ${rightNode.variableName} is not a node")

    val rightNodesMap = df_r.records.toList.map{ record =>
      record(rightNodesIndex).asInstanceOf[LynxNode].id -> record
    }.toMap
    val rightRecordsLen = df_r.schema.length
    val rightNodes = rightNodesMap.values.map(_(rightNodesIndex).asInstanceOf[LynxNode]).toList
    val relTypeName = rel.types.headOption
    DataFrame(schema, () => {
      df_l.records.flatMap{ record =>
        val endpoint = record.last match {
          case p: LynxPath => p.nodes.last
          case n: LynxNode => n
        }
        val rsl = inferExecutor.infer(endpoint, rightNodes).toList.filter(_._2.relationType == relTypeName)
        val out = if (optional&&rsl.isEmpty) {
          List((endpoint, LynxNull, LynxNull))
        } else rsl
        out.map { case (_, rel, r) => record ++ Seq(rel) ++ (r match {
            case n:LynxNode =>rightNodesMap(n.id)
            case _ => Seq.fill(rightRecordsLen)(LynxNull)
          })
        }
      }
    })
  }
}

case class InferLabel(nodeVariable: String)(implicit val plannerContext: PhysicalPlannerContext)
  extends SinglePhysicalPlan with InferPhysicalPlan {
  override def execute(implicit ctx: ExecutionContext): DataFrame = profile {
    val df = in.execute(ctx)
    val inferCondition = Condition(Seq.empty, Seq.empty, Seq.empty)
    val index = df.columnsName.indexOf(nodeVariable)
    val inferExecutor = ctx.inferEngine.adviser.forLabel(inferCondition)

    if (inferExecutor.isEmpty) throw NotMatchInferExecutorFoundException(inferCondition.toString)

    DataFrame(schema, () => {
      df.records.map { record =>
        val n = record(index).asInstanceOf[LynxNode]
        val newNode = inferExecutor.get.infer(n)
        record.updated(index, newNode)
      }
    })
  }
}


case class InferProperties(nodeVariable: String, propertyKey: Seq[LynxPropertyKey])(implicit val plannerContext: PhysicalPlannerContext)
  extends SinglePhysicalPlan with InferPhysicalPlan {

  override def execute(implicit ctx: ExecutionContext): DataFrame = profile {
    val df = in.execute(ctx)

    val inferCondition = Condition(Seq.empty, propertyKey.map(_.value), Seq.empty)

    val index = df.columnsName.indexOf(nodeVariable)

    val inferExecutor = ctx.inferEngine.adviser.forProperty(inferCondition)

    DataFrame(schema, () => {
      df.records.map{ record =>
        val n = record(index).asInstanceOf[LynxNode]
        if (propertyKey.forall(n.keys.contains)) {
          record
        } else {
          if (inferExecutor.isEmpty) throw NotMatchInferExecutorFoundException(inferCondition.toString)
          val newNode = inferExecutor.get.infer(n, propertyKey)
          record.updated(index, newNode)
        }
      }
    })
  }
}

