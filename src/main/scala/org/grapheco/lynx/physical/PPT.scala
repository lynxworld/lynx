package org.grapheco.lynx.physical

import org.grapheco.lynx.dataframe.{DataFrame, InnerJoin, JoinType}
import org.grapheco.lynx.evaluator.ExpressionContext
import org.grapheco.lynx.logical.plans
import org.grapheco.lynx.logical.plans.{LogicalApply, LogicalPatternMatch, LogicalShortestPaths, LogicalWith}
import org.grapheco.lynx.physical.planner.PPTNodeTranslator
import org.grapheco.lynx.physical.plans.{AbstractPhysicalPlan, PPTExpandPath, PPTMerge, PhysicalPlan, PPTNodeScan, PPTRelationshipScan, PPTRemove, PPTSetClause, PPTShortestPath, PPTUnwind}
import org.grapheco.lynx.procedure.{UnknownProcedureException, WrongArgumentException}
import org.grapheco.lynx.runner.{CONTAINS, EQUAL, ExecutionContext, GREATER_THAN, GREATER_THAN_OR_EQUAL, GraphModel, IN, LESS_THAN, LESS_THAN_OR_EQUAL, NOT_EQUAL, NodeFilter, PropOp, RelationshipFilter}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxCompositeValue, LynxList, LynxMap}
import org.grapheco.lynx.types.property.{LynxBoolean, LynxInteger, LynxNull, LynxNumber, LynxString}
import org.grapheco.lynx.types.spatial.LynxPoint
import org.grapheco.lynx.types.structural._
import org.grapheco.lynx.types.time.LynxTemporalValue
import org.grapheco.lynx.{LynxType, physical, runner}
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.InputPosition
import org.opencypher.v9_0.util.symbols.{CTAny, CTList, CTNode, CTPath, CTRelationship}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.{implicitConversions, postfixOps}

object Trans{
  implicit def labelToLynxLabel(l: LabelName): LynxNodeLabel = LynxNodeLabel(l.name)
}
































case class PPTCreateUnit(items: Seq[ReturnItem])(val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {
  override def withChildren(children0: Seq[PhysicalPlan]): PPTCreateUnit = PPTCreateUnit(items)(plannerContext)

  override val schema: Seq[(String, LynxType)] =
    items.map(item => item.name -> typeOf(item.expression))

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    createUnitDataFrame(items)
  }
}











case class PPTCreateIndex(labelName: String, properties: List[String])(implicit val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    graphModel._helper.createIndex(labelName, properties.toSet)
    DataFrame.empty
  }

  override def withChildren(children0: Seq[PhysicalPlan]): PhysicalPlan = this

  override val schema: Seq[(String, LynxType)] = {
    Seq("CreateIndex" -> CTAny)
  }
}




///////////////////////merge/////////////




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



case class PPTCreate(schemaLocal: Seq[(String, LynxType)], ops: Seq[FormalElement])(implicit val in: Option[PhysicalPlan], val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {
  override val children: Seq[PhysicalPlan] = in.toSeq

  override def withChildren(children0: Seq[PhysicalPlan]): PPTCreate = PPTCreate(schemaLocal, ops)(children0.headOption, plannerContext)

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







////////////////////










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





/////////////////////////////////


