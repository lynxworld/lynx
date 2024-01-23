package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner._
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.structural.{LynxId, LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.lynx.{LynxType, runner}
import org.opencypher.v9_0.expressions.{Expression, LabelName, ListLiteral, LogicalVariable, NodePattern, Range, RelTypeName, RelationshipPattern, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTList, CTNode, CTPath, CTRelationship}

case class ShortestPath(rel: RelationshipPattern, leftNode: NodePattern, rightNode: NodePattern, single: Boolean, resName: String)(val plannerContext: PhysicalPlannerContext) extends LeafPhysicalPlan {


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
      val tuples = Seq(
        var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
        var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
        var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode,
      )
      tuples
    }
    else {
      val tuples1 = Seq(
        var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
        var2.map(_.name).getOrElse(s"__RELATIONSHIP_LIST_${rel.hashCode}") -> CTList(CTRelationship),
        var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode,
        resName -> CTPath,
      )
      val tuples = tuples1
      tuples
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

    val (lowerLimit, upperLimit) = length match {
      case None => (1, 1)
      case Some(None) => (1, Int.MaxValue)
      case Some(Some(Range(a, b))) => (a.map(_.value.toInt).getOrElse(1), b.map(_.value.toInt).getOrElse(Int.MaxValue))
    }
    val (leftProperties, leftProps) = if (props1.isEmpty) (Map.empty[LynxPropertyKey, LynxValue], Map.empty[LynxPropertyKey, PropOp])
    else props1.get match {
      case li@ListLiteral(expressions) =>
        (eval(expressions(0)).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))
          , eval(expressions(1)).asInstanceOf[LynxMap].value.map(kv => {
          val v_2: PropOp = kv._2.value.toString match {
            case "IN" => IN
            case "EQUAL" => EQUAL
            case "NOTEQUALS" => NOT_EQUAL
            case "LessThan" => LESS_THAN
            case "LessThanOrEqual" => LESS_THAN_OR_EQUAL
            case "GreaterThan" => GREATER_THAN
            case "GreaterThanOrEqual" => GREATER_THAN_OR_EQUAL
            case "Contains" => CONTAINS
            case _ => throw new scala.Exception("unexpected PropOp" + kv._2.value)
          }
          (LynxPropertyKey(kv._1), v_2)
        }))
    }
    val (rightProperties, rightProps) = if (props3.isEmpty) (Map.empty[LynxPropertyKey, LynxValue], Map.empty[LynxPropertyKey, PropOp])
    else props3.get match {
      case li@ListLiteral(expressions) =>
        (eval(expressions(0)).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))
          , eval(expressions(1)).asInstanceOf[LynxMap].value.map(kv => {
          val v_2: PropOp = kv._2.value.toString match {
            case "IN" => IN
            case "EQUAL" => EQUAL
            case "NOTEQUALS" => NOT_EQUAL
            case "LessThan" => LESS_THAN
            case "LessThanOrEqual" => LESS_THAN_OR_EQUAL
            case "GreaterThan" => GREATER_THAN
            case "GreaterThanOrEqual" => GREATER_THAN_OR_EQUAL
            case "Contains" => CONTAINS
            case _ => throw new scala.Exception("unexpected PropOp" + kv._2.value)
          }
          (LynxPropertyKey(kv._1), v_2)
        }))
    }

    val types1 = types.map(_.name).map(LynxRelationshipType)
    val properties = props2.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)
    val startNodeFilter = runner.NodeFilter(labels1.map(_.name).map(LynxNodeLabel), leftProperties, leftProps)
    val endNodeFilter = runner.NodeFilter(labels3.map(_.name).map(LynxNodeLabel), rightProperties, rightProps)
    val startNodeId: LynxId = graphModel.nodes(startNodeFilter).map(x => x.id).toList.head
    val endNodeId: LynxId = graphModel.nodes(endNodeFilter).map(x => x.id).toList.head
    if (single) { // shortestPath(...)
      DataFrame(schema, () => {
        val paths = graphModel.singleShortestPath(startNodeId, endNodeId,
          RelationshipFilter(types1, properties), direction, lowerLimit, upperLimit)
        val it = Iterator(paths)
        if (length.isEmpty) {
          it.map { path => Seq(path.startNode.get, path.firstRelationship.get, path.endNode.get) }
        }
        else it.map { path => Seq(path.startNode.get, LynxList(path.relationships), path.endNode.get, path) }
      })
    }
    else { // allShortestPaths(...)
      DataFrame(schema, () => {
        val paths = graphModel.allShortestPaths(startNodeId, endNodeId,
          RelationshipFilter(types1, properties), direction, lowerLimit, upperLimit).iterator
        if (length.isEmpty) paths.map { path => Seq(path.startNode.get, path.firstRelationship.get, path.endNode.get) }
        else paths.map { path => Seq(path.startNode.get, LynxList(path.relationships), path.endNode.get, path) }
      })
    }
  }
}
