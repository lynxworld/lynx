package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.{LTNode, LTRelationship, LazyLynxValue, LynxType, LynxValue}
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner._
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.structural._
import org.opencypher.v9_0.expressions.{Expression, LabelName, ListLiteral, LogicalVariable, MapExpression, NodePattern, Range, RelTypeName, RelationshipPattern, SemanticDirection}

import scala.collection.parallel.ParSeq

case class Expand(rel: RelationshipPattern, rightNode: NodePattern, optional: Boolean = false)(l: PhysicalPlan, val plannerContext: PhysicalPlannerContext)
  extends SinglePhysicalPlan(l) {

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
    val ec = ctx.expressionContext

    val (lowerLimit, upperLimit) = length match {
      case None => (1, 1)
      case Some(None) => (1, Int.MaxValue)
      case Some(Some(Range(a, b))) => (a.map(_.value.toInt).getOrElse(1), b.map(_.value.toInt).getOrElse(Int.MaxValue))
    }

    val endLabel = if(labels2.nonEmpty) labels2.head.name else ""

    val newSchema = df.schema.filterNot(kv => schema0.map(_._1).contains(kv._1)) ++ schema0
    DataFrame(newSchema, () => {
      val result: Iterator[Seq[LynxValue]] = df.records.grouped(1000).flatMap(recordSeq => {
        recordSeq.par.map(record => {
          val recordMap = df.columnsName.zip(record).toMap
          val recordCtx = ec.withVars(recordMap)
          val recordToAdd = df.columnsName.zip(record).filterNot(kv => schema0.map(_._1).contains(kv._1)).map(_._2)
          val path = record.last match {
            case l: LazyLynxValue => val v = l.underLying
              v match {
                case p: LynxPath => p
                case n: LynxNode => LynxPath.startPoint(n)
              }
            case p: LynxPath => p
            case n: LynxNode => LynxPath.startPoint(n)
            case LynxNull => if(optional) null
            else throw new Exception("Expand Path is Null")
          }


          var exdResult: Iterator[Seq[LynxValue]] = Iterator.empty
          if(path == null && optional){
            exdResult = Seq(recordToAdd ++ schema0.map(col =>recordMap.getOrElse(col._1, LynxNull))).toIterator
          }else{
            val exd: Iterator[LynxPath] = graphModel.varExpandWithLabel(
              path.endNode.get,
              RelationshipFilter(types.map(_.name).map(LynxRelationshipType), properties.map(eval(_)(recordCtx).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))).getOrElse(Map.empty)),
              direction, Math.min(upperLimit, 10), lowerLimit, endLabel
            )

            val filterExpr = getNodeFilerProperties(properties2, recordCtx)
            val endNodeFilter = NodeFilter(labels2.map(_.name).map(LynxNodeLabel), Map.empty, filterExpr)

            exdResult = exd.grouped(1000).flatMap(exds => {
              val l: ParSeq[Seq[LynxValue]] = exds.par.filter(_.endNode.forall(endNodeFilter.matches(_)))
                .map { path =>
                  if(path.relationships.length == 1)
                    recordToAdd.:+(path.relationships.head).:+(path.endNode.get)
                  else
                    recordToAdd.:+(LynxList(path.relationships)).:+(path.endNode.get)
                }
              l
            })
            if(exdResult.isEmpty && optional){
              val recordMap = df.columnsName.zip(record).toMap
              exdResult = Seq(recordToAdd++ schema0.map(s => recordMap.getOrElse(s._1, LynxNull))).toIterator
            }
          }
          exdResult
        }).reduceOption(_ ++ _).getOrElse(Iterator.empty)
      })
      result
    }
    )
  }
}
