package org.grapheco.lynx.logical.planner.translators
import org.opencypher.v9_0.expressions.{AnonymousPatternPart, Equals, EveryPath, Expression, HasLabels, LabelName, NamedPatternPart, NodePattern, Pattern, PatternElement, PatternPart, RelationshipChain, RelationshipPattern, ShortestPaths}
import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{GraphPattern, GraphPatternEdge, GraphPatternMatch, GraphPatternNode, LogicalPlan}
import org.opencypher.v9_0.ast.{Match, Where}
import org.grapheco.lynx.logical.plans.ASTConvertor._
import org.grapheco.lynx.types.structural.LynxNodeLabel

import scala.language.implicitConversions

case class MatchTranslator_GB(m: Match) extends LogicalTranslator {

  override def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    val graphPattern: GraphPattern = in match {
      case Some(value: GraphPatternMatch) => value.graphPattern
      case _ => new GraphPattern
    }
    val Match(optional, Pattern(patternParts: Seq[PatternPart]), hints, where: Option[Where]) = m
    patternParts.foreach{
      // case EveryPath(element) => translatePattern(None, element, optional)(graphPattern)
      case EveryPath(element) => translatePattern(Some(element.variable.get.name), element, optional,where)(graphPattern)
      case ShortestPaths(element, single) => None //TODO graph pattern not support shortest paths
      case NamedPatternPart(variable, patternPart) => patternPart match {
        case EveryPath(element) => translatePattern(Option(variable.name), element, optional,where)(graphPattern)
        case ShortestPaths(element, single) => None //TODO graph pattern not support shortest paths
      }
    }
    GraphPatternMatch(graphPattern)
  }

  private def translatePattern(variableName: Option[String], element: PatternElement, optional: Boolean,where:Option[Where])(graphPattern: GraphPattern): Unit = element match {
    // TODO variable name for relationship chain
      //match ()
//      case n:NodePattern => graphPattern.addNode(
//        where match{
//          case Some(Where(HasLabels(_,labels))) =>
//            n.withLabels(labels.map(label => LynxNodeLabel(label.name)))
//          case None => n
//        }
//      )
    case n: NodePattern => {
      var node: GraphPatternNode = n
      where match{
        case None =>
        case Some(Where(HasLabels(_, labels))) =>node =node.withLabels(labels.map(label => LynxNodeLabel(label.name)))
        case _=> where.get.expression.subExpressions.foreach { exp =>
          exp match {
            case HasLabels(_, labels) => node = node.withLabels(labels.map(label => LynxNodeLabel(label.name)))
            case Equals(_, _) => node = node.withProperties(exp)
            case _ =>
          }
        }
      }
      graphPattern.addNode(node)
    }
//      graphPattern.addNode(
//        where.get.expression.subExpressions match {
//          case Some(Where(HasLabels(_, labels))) =>
//            n.withLabels(labels.map(label => LynxNodeLabel(label.name)))
//          case None => n
//        }
//      )
      //match ()-[]->()
      case RelationshipChain(s: NodePattern, r: RelationshipPattern, t: NodePattern) => graphPattern.addEdge(s,r,t)
      //match ()-[]->()-...-[]->()
      case rc@RelationshipChain(leftChain: RelationshipChain, r: RelationshipPattern, t: NodePattern) =>
        translatePattern(None, leftChain, optional,where)(graphPattern)
        graphPattern.addEdge(leftChain.rightNode,r,t)
    }
}
