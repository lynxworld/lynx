package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.logical.plans.{GraphPatternEdge, GraphPatternNode}
import org.grapheco.lynx.physical.{NodeInput, PhysicalPlannerContext}
import org.grapheco.lynx.runner.infer.{Condition, NotMatchInferExecutorFoundException}
import org.grapheco.lynx.runner.{CONTAINS, EQUAL, ExecutionContext, GREATER_THAN, GREATER_THAN_OR_EQUAL, IN, LESS_THAN, LESS_THAN_OR_EQUAL, NOT_EQUAL, NodeFilter, PropOp, RelationshipFilter}
import org.grapheco.lynx.types.{LTNode, LTVNode, LTVRelationship, LynxType, LynxValue}
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPath, LynxPropertyKey, LynxRelationshipType}
import org.opencypher.v9_0.expressions.{And, Ands, BinaryOperatorExpression, Equals, Expression, HasLabels, LabelName, ListLiteral, LogicalVariable, NodePattern, Property, PropertyKeyName, Range, RelTypeName, RelationshipPattern, SemanticDirection, Variable, VirtualNodePattern, VirtualRelationshipPattern}
import org.opencypher.v9_0.util.InputPosition

trait InferPhysicalPlan extends SinglePhysicalPlan

case class InferPlanner()(implicit val plannerContext: PhysicalPlannerContext) {
  def filters(node: GraphPatternNode)(in: PhysicalPlan): Seq[PhysicalPlan] = {
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
    case _ => Map.empty
    // other
  }
}

case class InferFakeNode(pattern: GraphPatternNode)(implicit val plannerContext: PhysicalPlannerContext) extends InferPhysicalPlan {
  override def schema: Seq[(String, LynxType)] = Seq((pattern.variableName, LTVNode))

  override def execute(implicit ctx: ExecutionContext): DataFrame = DataFrame.empty
}

case class InferExpand(rel: GraphPatternEdge, rightNode: GraphPatternNode)(implicit val plannerContext: PhysicalPlannerContext)
  extends InferPhysicalPlan {

  override def schema: Seq[(String, LynxType)] = in.schema ++ Seq(
      rel.variableName -> LTVRelationship,
      rightNode.variableName -> LTVNode)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)

    val inferCondition = Condition(rightNode.labels.map(_.toString), Seq.empty, rel.types.map(_.toString))

    var step = 0

    DataFrame(schema, () => {
      df.records.flatMap{ record =>
        val endpoint = record.last match {
          case p: LynxPath => p.nodes.last
          case n: LynxNode => n
        }
        println(s"infer step: $step")
        step += 1
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

case class InferLabel(nodeVariable: String)(implicit val plannerContext: PhysicalPlannerContext)
  extends InferPhysicalPlan {
  override def execute(implicit ctx: ExecutionContext): DataFrame = {
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
  extends InferPhysicalPlan {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)

    val inferCondition = Condition(Seq.empty, propertyKey.map(_.value), Seq.empty)

    val index = df.columnsName.indexOf(nodeVariable)

    val inferExecutor = ctx.inferEngine.adviser.forProperty(inferCondition)

    if (inferExecutor.isEmpty) throw NotMatchInferExecutorFoundException(inferCondition.toString)

    DataFrame(schema, () => {
      df.records.map{ record =>
        val n = record(index).asInstanceOf[LynxNode]
        val newNode = inferExecutor.get.infer(n)
        record.updated(index, newNode)
      }
    })
  }
}

