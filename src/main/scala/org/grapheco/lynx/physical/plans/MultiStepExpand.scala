package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.{LTNode, LTRelationship, LynxType, LynxValue}
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner._
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.structural._
import org.opencypher.v9_0.expressions.{Expression, LabelName, ListLiteral, LogicalVariable, NodePattern, Range, RelTypeName, RelationshipPattern, SemanticDirection}

case class MultiStepExpand(rel: RelationshipPattern,
                           rightNode: NodePattern
                          )(implicit in: PhysicalPlan, val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {

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
    val schema0 = Seq(variable.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> LTRelationship,
      var2.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> LTNode)
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

    val schema0 = Seq(variable.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> LTRelationship,
      var2.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> LTNode)

    implicit val ec = ctx.expressionContext

    val filterExpr = getNodeFilerProperties(properties2, ec)

    val (lowerLimit, upperLimit) = length match {
      case None => (1, 1)
      case Some(None) => (1, Int.MaxValue)
      case Some(Some(Range(a, b))) => (a.map(_.value.toInt).getOrElse(1), b.map(_.value.toInt).getOrElse(Int.MaxValue))
    }
    val endNodeFilter = NodeFilter(labels2.map(_.name).map(LynxNodeLabel), Map.empty, filterExpr)

    DataFrame(df.schema ++ schema0, () => {
      df.records.flatMap {
        record =>
          val path = record.last match {
            case p: LynxPath => p
            case n: LynxNode => LynxPath.startPoint(n)
          }

          val exd = graphModel.varExpand(
              path.endNode.get,
              RelationshipFilter(types.map(_.name).map(LynxRelationshipType), properties.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
              direction, Math.min(upperLimit, 10), lowerLimit
            )
            .filter(_.endNode.forall(endNodeFilter.matches(_)))

          exd.map { path =>
            record.:+(LynxList(path.relationships)).:+(path.endNode.get)
          }
        //            .filter(item => { // TODO: rewrite this filter as a PPT
        //              //(m)-[r]-(n)-[p]-(t), r!=p
        //              val relIds = item.filter(_.isInstanceOf[LynxRelationship]).map(_.asInstanceOf[LynxRelationship].id)
        //              relIds.size == relIds.toSet.size
        //            })
      }
    })
  }
}
