package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner._
import org.grapheco.lynx.types.{LTList, LTNode, LTPath, LTRelationship, LynxType, LynxValue}
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPath, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.lynx.runner
import org.opencypher.v9_0.expressions.{Expression, LabelName, ListLiteral, LogicalVariable, NodePattern, Range, RelTypeName, RelationshipPattern, SemanticDirection}

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
        var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> LTNode,
        var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> LTRelationship,
        var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> LTNode,
      )
      tuples
    }
    else {
      val tuples1 = Seq(
        var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> LTNode,
        var2.map(_.name).getOrElse(s"__RELATIONSHIP_LIST_${rel.hashCode}") -> LTList(LTRelationship),
        var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> LTNode,
        resName -> LTPath,
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

    val ec = ctx.expressionContext
    val df = ctx.arguments
    val newSchema = df.schema.filterNot(col => schema.map(_._1).contains(col._1)) ++ schema

    val (lowerLimit, upperLimit) = length match {
      case None => (1, 1)
      case Some(None) => (1, Int.MaxValue)
      case Some(Some(Range(a, b))) => (a.map(_.value.toInt).getOrElse(1), b.map(_.value.toInt).getOrElse(Int.MaxValue))
    }
    val types1 = types.map(_.name).map(LynxRelationshipType)

    // shortestPath(...)
    DataFrame(newSchema, () => {
      if(df.schema.size!=0){
        df.records.flatMap(record => {
          val recordCtx = ec.withVars(df.columnsName.zip(record).toMap)
          val leftFilterExpr = getNodeFilerProperties(props1, recordCtx)
          val rightFilterExpr = getNodeFilerProperties(props3, recordCtx)
          val properties = props2.map(eval(_)(recordCtx).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)
          val startNodes = graphModel.nodes(runner.NodeFilter(labels1.map(_.name).map(LynxNodeLabel), Map.empty, leftFilterExpr))
          val endNodes = graphModel.nodes(runner.NodeFilter(labels3.map(_.name).map(LynxNodeLabel), Map.empty, rightFilterExpr))
          val it: Iterator[LynxPath] = startNodes.flatMap(startNode => {
            if(single){
              endNodes.map(endNode => {
                graphModel.singleShortestPath(startNode.id, endNode.id,
                  RelationshipFilter(types1, properties), direction, lowerLimit, upperLimit)
              })
            }else{
              endNodes.flatMap(endNode => {
                graphModel.allShortestPaths(startNode.id, endNode.id,
                  RelationshipFilter(types1, properties), direction, lowerLimit, upperLimit)
              })
            }

          }).filter(p => p.elements.nonEmpty)
          if (length.isEmpty) {
            it.map { path => Seq(path.startNode.get, path.firstRelationship.get, path.endNode.get) }
          }
          else it.map { path => Seq(path.startNode.get, LynxList(path.relationships), path.endNode.get, path) }
        }.map(df.columnsName.zip(record).filterNot(col => schema.map(_._1).contains(col._1) ).map(_._2) ++ _))
      }else{
        val leftFilterExpr = getNodeFilerProperties(props1, ec)
        val rightFilterExpr = getNodeFilerProperties(props3, ec)
        val properties = props2.map(eval(_)(ec).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)

        val startNodeFilter = runner.NodeFilter(labels1.map(_.name).map(LynxNodeLabel), Map.empty, leftFilterExpr)
        val startNodes = graphModel.nodes(startNodeFilter)

        val endNodeFilter = runner.NodeFilter(labels3.map(_.name).map(LynxNodeLabel), Map.empty, rightFilterExpr)
        val endNodes = graphModel.nodes(endNodeFilter)
        val it: Iterator[LynxPath] = startNodes.flatMap(startNode => {
          if(single){
            endNodes.map(endNode => {
              graphModel.singleShortestPath(startNode.id, endNode.id,
                RelationshipFilter(types1, properties), direction, lowerLimit, upperLimit)
            })
          }else{
            endNodes.flatMap(endNode => {
              graphModel.allShortestPaths(startNode.id, endNode.id,
                RelationshipFilter(types1, properties), direction, lowerLimit, upperLimit)
            })
          }
        }).filter(p => p.elements.nonEmpty)
        if (length.isEmpty) {
          it.map { path => Seq(path.startNode.get, path.firstRelationship.get, path.endNode.get) }
        }
        else it.map { path => Seq(path.startNode.get, LynxList(path.relationships), path.endNode.get, path) }
      }
    })
  }
}
