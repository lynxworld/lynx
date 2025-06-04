package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.{NodeInput, PhysicalPlannerContext}
import org.grapheco.lynx.runner.infer.{Condition, NotMatchInferExecutorFoundException}
import org.grapheco.lynx.runner.{CONTAINS, EQUAL, ExecutionContext, GREATER_THAN, GREATER_THAN_OR_EQUAL, IN, LESS_THAN, LESS_THAN_OR_EQUAL, NOT_EQUAL, NodeFilter, PropOp, RelationshipFilter}
import org.grapheco.lynx.types.{LTNode, LTVNode, LTVRelationship, LynxType, LynxValue}
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPath, LynxPropertyKey, LynxRelationshipType}
import org.opencypher.v9_0.expressions.{Expression, LabelName, ListLiteral, LogicalVariable, NodePattern, Range, RelTypeName, RelationshipPattern, SemanticDirection, VirtualNodePattern, VirtualRelationshipPattern}

case class InferExpand(rel: VirtualRelationshipPattern, rightNode: VirtualNodePattern)(implicit val plannerContext: PhysicalPlannerContext)
  extends SinglePhysicalPlan {

  override def schema: Seq[(String, LynxType)] = in.schema ++ Seq(
      rel.variable.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> LTVRelationship,
      rightNode.variable.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> LTVNode)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)

    val inferCondition = Condition(rightNode.labels.map(_.name), Seq.empty, rel.types.map(_.name))

    DataFrame(schema, () => {
      df.records.flatMap{ record =>
        val endpoint = record.last match {
          case p: LynxPath => p.nodes.last
          case n: LynxNode => n
        }

        val inferEngine = ctx.inferEngine
        val inferExecutor =  inferEngine.adviser.forExpand(inferCondition)
        if(inferExecutor.isDefined) {
          val expand = inferExecutor.get
          val rsl = expand.infer(endpoint)
          rsl.map { case (relationship, node) =>
//              graphModel.write.createElements(
//                Seq(("", NodeInput(node.labels, node))), Seq(("", RelationshipInput(relationship.relType, relationship.startNodeId, relationship.endNodeId, relationship.props.toSeq))))
              record ++ Seq(relationship, node)}
        } else {
          throw  NotMatchInferExecutorFoundException()
        }
      }
    })
  }
}

case class InferProperties(nodeVariable: LogicalVariable, propertyKey: Seq[LynxPropertyKey])(implicit val plannerContext: PhysicalPlannerContext)
  extends SinglePhysicalPlan {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)

    val inferCondition = Condition(Seq.empty, propertyKey.map(_.value), Seq.empty)

    val index = df.columnsName.indexOf(nodeVariable.name)

    val inferExecutor = ctx.inferEngine.adviser.forProperty(inferCondition)

    if (inferExecutor.isEmpty) throw NotMatchInferExecutorFoundException()

    DataFrame(schema, () => {
      df.records.map{ record =>
        val n = record(index).asInstanceOf[LynxNode]
        val newNode = inferExecutor.get.infer(n)
        record.updated(index, newNode)
      }
    })
  }
}

case class InferGraph()

