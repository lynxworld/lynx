package org.grapheco.lynx.physical

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.plans.{AbstractPhysicalPlan, PhysicalPlan}
import org.grapheco.lynx.runner.{ExecutionContext, GraphModel}
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.structural._
import org.grapheco.lynx.types.{LynxType, LynxValue}
import org.opencypher.v9_0.expressions.SemanticDirection.{INCOMING, OUTGOING}
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.InputPosition

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.{implicitConversions, postfixOps}

object Trans{
  implicit def labelToLynxLabel(l: LabelName): LynxNodeLabel = LynxNodeLabel(l.name)
}





















///////////////////////merge/////////////




/////////////////////////////////////////

trait FormalElement {
  def varName: String
}

case class FormalNode(varName: String, labels: Seq[LabelName], properties: Option[Expression]) extends FormalElement

case class FormalRelationship(varName: String, types: Seq[RelTypeName], properties: Option[Expression], varNameLeftNode: String, varNameRightNode: String, direction: SemanticDirection = OUTGOING) extends FormalElement

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

      case FormalRelationship(varName: String, types: Seq[RelTypeName], properties: Option[Expression], varNameLeftNode: String, varNameRightNode: String, direction: SemanticDirection) =>

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



case class PPTCreate(schemaLocal: Seq[(String, LynxType)], ops: Seq[FormalElement])(l: Option[PhysicalPlan], val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalPlan(l) {
  def in: Option[PhysicalPlan] = this.left
  override def schema: Seq[(String, LynxType)] = in.map(_.schema).getOrElse(Seq.empty) ++ schemaLocal

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

          case FormalRelationship(varName: String, types: Seq[RelTypeName], properties: Option[Expression], varNameLeftNode: String, varNameRightNode: String, direction: SemanticDirection) =>

            def nodeInputRef(varname: String): NodeInputRef = {
              ctxMap.get(varname).map(
                x =>
                  StoredNodeInputRef(x.asInstanceOf[LynxNode].id)
              ).getOrElse(
                ContextualNodeInputRef(varname)
              )
            }
            val relationshipInput: RelationshipInput = direction match {
              case INCOMING => RelationshipInput(types.map(_.name).map(LynxRelationshipType),
                properties.map(eval(_)(ec.withVars(ctxMap))).map {
                  case LynxMap(m) => m.map { case (str, value) => LynxPropertyKey(str) -> value }.toSeq
                  case _ => throw SyntaxErrorException("Property should be a Map.")
                }.getOrElse(Seq.empty), nodeInputRef(varNameRightNode), nodeInputRef(varNameLeftNode))
              case OUTGOING => RelationshipInput(types.map(_.name).map(LynxRelationshipType),
                properties.map(eval(_)(ec.withVars(ctxMap))).map {
                  case LynxMap(m) => m.map { case (str, value) => LynxPropertyKey(str) -> value }.toSeq
                  case _ => throw SyntaxErrorException("Property should be a Map.")
                }.getOrElse(Seq.empty), nodeInputRef(varNameLeftNode), nodeInputRef(varNameRightNode))
              case _ => throw new Exception("Only directed relationships are supported in CREATE")
            }
            relsInput += varName -> relationshipInput
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


