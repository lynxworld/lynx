package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner._
import org.grapheco.lynx.types.{LTList, LTNode, LTPath, LTRelationship, LynxType, LynxValue}
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.lynx.runner
import org.grapheco.lynx.types.property.LynxNull
import org.opencypher.v9_0.expressions.{Expression, LabelName, ListLiteral, LogicalVariable, NodePattern, Range, RelTypeName, RelationshipPattern, SemanticDirection}

case class RelationshipScan(rel: RelationshipPattern, leftNode: NodePattern, rightNode: NodePattern, optional: Boolean = false)(implicit val plannerContext: PhysicalPlannerContext) extends LeafPhysicalPlan {

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
        var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> LTNode,
        var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> LTRelationship,
        var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> LTNode,
      )
    }
    else {
      Seq(
        var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> LTNode,
        var2.map(_.name).getOrElse(s"__RELATIONSHIP_LIST_${rel.hashCode}") -> LTList(LTRelationship),
        var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> LTNode,
        var2.map(_.name + "LINK").getOrElse(s"__LINK_${rel.hashCode}") -> LTPath
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
    val ec = ctx.expressionContext

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
    val df = ctx.arguments
    val newSchema = df.schema.filterNot(col => schema.map(_._1).contains(col._1)) ++ schema

    DataFrame(newSchema,() => {
      if(df.schema.size!=0){
        df.records.grouped(1000).flatMap(records =>{
          records.par.map(record => {
            val recordCtx = ec.withVars(df.columnsName.zip(record).toMap)
            val leftFilterExpr = getNodeFilerProperties(props1, recordCtx)
            val rightFilterExpr = getNodeFilerProperties(props3, recordCtx)

            val paths = graphModel.paths(
              runner.NodeFilter(labels1.map(_.name).map(LynxNodeLabel), Map.empty, leftFilterExpr),
              runner.RelationshipFilter(types.map(_.name).map(LynxRelationshipType), props2.map(eval(_)(recordCtx).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
              runner.NodeFilter(labels3.map(_.name).map(LynxNodeLabel), Map.empty, rightFilterExpr),
              direction, upperLimit, lowerLimit)
            val iterResult = if (length.isEmpty) paths.map { path => Seq(path.startNode.get, path.firstRelationship.get, path.endNode.get) }
            else paths.map { path => Seq(path.startNode.get, LynxList(path.relationships), path.endNode.get, path) }
            val iter: Iterator[Seq[LynxValue]] = iterResult.map(df.columnsName.zip(record).filterNot(col => schema.map(_._1).contains(col._1)).map(_._2) ++ _)
            if (iter.isEmpty && optional) {
              val recordMap = df.columnsName.zip(record).toMap
              val recordToAdd = schema.map(col => if (df.columnsName.contains(col._1)) recordMap.get(col._1).getOrElse(LynxNull) else LynxNull)
              Seq(recordMap.toList.filterNot(col => schema.map(_._1).contains(col._1)).map(_._2) ++ recordToAdd).toIterator
            }
            else iter
            //        else paths.map { path => Seq(path.startNode.get, LynxList(path.relationships), path.endNode.get, path.trim) } // fixme: huchuan 2023-04-11: why trim?
          }).reduceOption(_ ++ _).getOrElse(Iterator.empty)
        })
      }else {
        val leftFilterExpr = getNodeFilerProperties(props1, ec)
        val rightFilterExpr = getNodeFilerProperties(props3, ec)
        val paths = graphModel.paths(
          runner.NodeFilter(labels1.map(_.name).map(LynxNodeLabel), Map.empty, leftFilterExpr),
          runner.RelationshipFilter(types.map(_.name).map(LynxRelationshipType), props2.map(eval(_)(ec).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
          runner.NodeFilter(labels3.map(_.name).map(LynxNodeLabel), Map.empty, rightFilterExpr),
          direction, upperLimit, lowerLimit)
        if (length.isEmpty) paths.map { path => Seq(path.startNode.get, path.firstRelationship.get, path.endNode.get) }
        else paths.map { path => Seq(path.startNode.get, LynxList(path.relationships), path.endNode.get, path) }
      }
    })
  }
}
