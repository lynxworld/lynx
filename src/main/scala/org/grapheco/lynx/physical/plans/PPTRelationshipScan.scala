package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner._
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.lynx.{LynxType, runner}
import org.opencypher.v9_0.expressions.{Expression, LabelName, ListLiteral, LogicalVariable, NodePattern, Range, RelTypeName, RelationshipPattern, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTList, CTNode, CTPath, CTRelationship}

case class PPTRelationshipScan(rel: RelationshipPattern, leftNode: NodePattern, rightNode: NodePattern)(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {
  override def withChildren(children0: Seq[PhysicalPlan]): PPTRelationshipScan = PPTRelationshipScan(rel, leftNode, rightNode)(plannerContext)

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

    val (leftProperties, leftProps) = if (props1.isEmpty) (Map.empty[LynxPropertyKey, LynxValue], Map.empty[LynxPropertyKey, PropOp])
    else props1.get match {
      case li@ListLiteral(expressions) => {
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
      case _ => {
        (props1.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty), Map.empty[LynxPropertyKey, PropOp])
      }

    }

    val (rightProperties, rightProps) = if (props3.isEmpty) (Map.empty[LynxPropertyKey, LynxValue], Map.empty[LynxPropertyKey, PropOp])
    else props3.get match {
      case li@ListLiteral(expressions) => {
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
      case _ => {
        (props3.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty), Map.empty[LynxPropertyKey, PropOp])
      }
    }
    println(runner.NodeFilter(labels1.map(_.name).map(LynxNodeLabel), leftProperties, leftProps))
    println(runner.NodeFilter(labels3.map(_.name).map(LynxNodeLabel), rightProperties, rightProps))
    DataFrame(schema,
      () => {
        val paths = graphModel.paths(
          runner.NodeFilter(labels1.map(_.name).map(LynxNodeLabel), leftProperties, leftProps),
          runner.RelationshipFilter(types.map(_.name).map(LynxRelationshipType), props2.map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
          runner.NodeFilter(labels3.map(_.name).map(LynxNodeLabel), rightProperties, rightProps),
          direction, upperLimit, lowerLimit)
        if (length.isEmpty) paths.map { path => Seq(path.startNode.get, path.firstRelationship.get, path.endNode.get) }
        else paths.map { path => Seq(path.startNode.get, LynxList(path.relationships), path.endNode.get, path) }
        //        else paths.map { path => Seq(path.startNode.get, LynxList(path.relationships), path.endNode.get, path.trim) } // fixme: huchuan 2023-04-11: why trim?

      }
    )
  }
}
