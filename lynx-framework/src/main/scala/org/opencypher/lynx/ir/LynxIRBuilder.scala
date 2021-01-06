package org.opencypher.lynx.ir

import org.opencypher.lynx.{EvalContext, LynxSession, RecordHeader}
import org.opencypher.lynx.graph.LynxPropertyGraph
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNull, CypherString, CypherValue}
import org.opencypher.okapi.ir.api.{CreateElementStatement, CypherStatement}
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext, QueryLocalCatalog}
import org.opencypher.v9_0.ast.{Create, Query, SingleQuery, Statement}
import org.opencypher.v9_0.expressions.{EveryPath, Expression, LabelName, LogicalVariable, MapExpression, NodePattern, PatternElement, RelationshipChain, Variable}
import scala.collection.mutable.ArrayBuffer

object LynxIRBuilder {
  def process[Id](statement: Statement, irBuilderContext: IRBuilderContext, lynxSession: LynxSession): (CypherStatement, QueryLocalCatalog) = {
    statement match {
      case Query(_, SingleQuery(Seq(Create(pattern)))) =>
        val nodes = ArrayBuffer[(Option[LogicalVariable], IRNode)]()
        val rels = ArrayBuffer[IRRelation[Id]]()

        implicit val evalContext = EvalContext(RecordHeader.empty, (_) => CypherNull, irBuilderContext.parameters)
        pattern.patternParts.foreach {
          case EveryPath(NodePattern(variable: Option[LogicalVariable], labels, properties, _)) =>
            nodes += variable -> IRNode(labels.map(_.name), properties.map {
              case MapExpression(items) =>
                items.map({
                  case (k, v) => k.name -> lynxSession.tableOperator.eval(irBuilderContext.convertExpression(v))
                })
            }.getOrElse(Seq.empty))

          case EveryPath(RelationshipChain(element, relationship, rightNode)) =>
            def nodeRef(pe: PatternElement): IRNodeRef[Id] = {
              pe match {
                case NodePattern(variable, _, _, _) =>
                  nodes.toMap.get(variable).map(IRContextualNodeRef[Id](_)).getOrElse(throw new UnrecognizedVarException(variable))
              }
            }

            rels += IRRelation[Id](relationship.types.map(_.name), relationship.properties.map {
              case MapExpression(items) =>
                items.map({
                  case (k, v) => k.name -> lynxSession.tableOperator.eval(irBuilderContext.convertExpression(v))
                })
            }.getOrElse(Seq.empty[(String, CypherValue)]),
              nodeRef(element),
              nodeRef(rightNode)
            )

          case _ =>
        }

        CreateElementStatement(nodes.map(_._2).toArray, rels.toArray) -> QueryLocalCatalog.empty

      case _ =>
        val irOut = IRBuilder.process(statement)(irBuilderContext)
        IRBuilder.extract(irOut) -> IRBuilder.getContext(irOut).queryLocalCatalog
    }
  }
}

case class IRNode(labels: Seq[String], props: Seq[(String, CypherValue)]) {

}

case class IRRelation[Id](types: Seq[String], props: Seq[(String, CypherValue)], startNodeRef: IRNodeRef[Id], endNodeRef: IRNodeRef[Id]) {

}

sealed trait IRNodeRef[Id]

case class IRStoredNodeRef[Id](id: Id) extends IRNodeRef[Id]

case class IRContextualNodeRef[Id](node: IRNode) extends IRNodeRef[Id]

trait PropertyGraphWriter[Id] {
  def createElements(nodes: Array[IRNode], rels: Array[IRRelation[Id]])
}

trait WritablePropertyGraph[Id] extends LynxPropertyGraph {
  def createElements(nodes: Array[IRNode], rels: Array[IRRelation[Id]])
}

class UnrecognizedVarException(var0: Option[LogicalVariable]) extends RuntimeException