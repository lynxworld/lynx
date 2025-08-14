package org.grapheco.lynx.logical.planner.translators
import org.opencypher.v9_0.expressions.{And, Ands, AnonymousPatternPart, Equals, EveryPath, Expression, HasLabels, LabelName, LogicalVariable, NamedPatternPart, NodePattern, Pattern, PatternElement, PatternPart, RelationshipChain, RelationshipPattern, ShortestPaths}
import org.grapheco.lynx.logical.{LogicalPlannerContext, ShortestPathNotSupported}
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans.{FilterExpression, GraphPattern, GraphPatternEdge, GraphPatternMatch, GraphPatternNode, LogicalAndThen, LogicalJoin, LogicalPlan, LogicalWith}
import org.opencypher.v9_0.ast.{Match, Where}
import org.grapheco.lynx.logical.plans.ASTConvertor._
import org.grapheco.lynx.types.structural.LynxNodeLabel

import scala.language.implicitConversions
import LynxNodeLabel.fromNodeLabel
import org.grapheco.lynx.LynxException
import org.grapheco.lynx.dataframe.{JoinType, LeftJoin, OuterJoin, RightJoin}
import org.grapheco.lynx.types.{LTNode, LTRelationship, LTVNode, LTVRelationship}

case class MatchTranslator_GB(m: Match) extends LogicalTranslator {

  var inputVariables:Seq[String] = Seq.empty

  override def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    inputVariables = plannerContext.variables.map(_._1)
    // Combine the input graph pattern with the current graph pattern
    (m.optional, in) match {
      // 1. Match-Match
      case (_, Some(p@GraphPatternMatch(gp, filters, optional))) =>
        def makeApply(joinType: JoinType): LogicalPlan = p.left match {
          case Some(w:LogicalWith) => LogicalAndThen(LeftJoin)(w, LogicalJoin(false, joinType)(p.alone, construct()))
          case _ => LogicalJoin(false, joinType)(p, construct())
        }
        (m.optional, optional) match {
          // 1.1 Optional-Optional => FullJoin
          case (true, true) =>  makeApply(OuterJoin)
          // 1.2 Optional-Match => LeftJoin
          case (true, false) => makeApply(LeftJoin)
          // 1.3 Match-Optional => RightJoin
          case (false, true) => makeApply(RightJoin)
          // 2. Match-Match => Combine
          case (false, false) => construct(gp, filters, p.left)
        }

      // 2. Match-Other => Normal
      case (false, _) => construct(in = in)
      // 3. Optional-Other => TODO
      case (true, None) => construct()
      case (true, Some(p)) => LogicalAndThen(LeftJoin)(p, construct())
    }
  }

  def construct(graphPattern: GraphPattern = new GraphPattern,
                filterOfIn: FilterExpression = FilterExpression.empty,
                in: Option[LogicalPlan]=None)(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    val Match(optional, Pattern(patternParts: Seq[PatternPart]), hints, where: Option[Where]) = m

    // Translate each pattern part and add it to the graph pattern
    patternParts.foreach{ // TODO variable name for relationship chain
      case EveryPath(element) => translatePattern(element, optional,where)(graphPattern)
      case ShortestPaths(_, _) => throw ShortestPathNotSupported() //TODO graph pattern not support shortest paths
      case NamedPatternPart(variable, patternPart) => patternPart match {
        case EveryPath(element) => translatePattern(element, optional,where)(graphPattern)
        case ShortestPaths(_, _) => throw ShortestPathNotSupported()  //TODO graph pattern not support shortest paths
      }
    }
    // Translate the WHERE clause if it exists, put it in the graph pattern, and return filters can not be translated.
    val filter = where.map(w => translateWhere(w.expression)(graphPattern)).getOrElse(FilterExpression(Map.empty))
    // Return the combined graph pattern
    plannerContext.variables = plannerContext.variables ++
      graphPattern.allNodes.map(n => (n.variableName, if(n.virtual) LTVNode else LTNode)) ++
      graphPattern.allEdges.map(e => (e.variableName, if(e.virtual) LTVRelationship else LTRelationship))
    GraphPatternMatch(graphPattern, filterOfIn combine filter, optional)(in)
  }

  private def translatePattern(element: PatternElement, optional: Boolean,where:Option[Where])(graphPattern: GraphPattern): Unit = element match {
    case n: NodePattern => graphPattern.addNode(n)
    case RelationshipChain(s: NodePattern, r: RelationshipPattern, t: NodePattern) => graphPattern.addEdge(s,r,t)
    case RelationshipChain(leftChain: RelationshipChain, r: RelationshipPattern, t: NodePattern) =>
      translatePattern(leftChain, optional, where)(graphPattern)
      graphPattern.addEdge(leftChain.rightNode,r,t)
  }


  private def translateWhere(expr: Expression)(g: GraphPattern): FilterExpression = expr match {
    // uncombined And&Ands
    case And(lhs, rhs) => translateWhere(lhs)(g) combine translateWhere(rhs)(g)
    case Ands(expressions) => expressions.map(translateWhere(_)(g)).reduce(_ combine _)
    // only push label&type to element
    case HasLabels(LogicalVariable(str), labels) => g.maybeNode(str)
      .map(_.addLabels(labels.map(fromNodeLabel)))
      .map(g.updateNode)
      .map(_ => FilterExpression.empty).getOrElse(FilterExpression(Map(Set(str) -> Seq(expr))))
    // push expression to single element, if the filter is only involved in one element(exclude input variables).
    case oneDependency if (oneDependency.dependencies.map(_.name)--inputVariables).size == 1 => attachSingleFilter(g, oneDependency)
    // One more dependency, can not be translated.
    case _ => FilterExpression(Map(expr.dependencies.map(_.name) -> Seq(expr)))
  }

  private def attachSingleFilter(g: GraphPattern, filter: Expression): FilterExpression = {
    val involvedName = filter.dependencies.head.name // size = 1
    // 0. If the filter is not involved in the graph pattern, return the filter expression.
    if (!g.containsElement(involvedName)) return FilterExpression(Map(Set(involvedName) -> Seq(filter)))

    // 1. attach the filter to the node
    val _node: Option[GraphPatternNode] = g.maybeNode(involvedName).map(_.addExpressions(Seq(filter)))
    if (_node.isDefined) {
      g.updateNode(_node.get)
      return FilterExpression.empty
    }
    // 2. attach the filter to the edge
    val _edge: Option[GraphPatternEdge] = g.maybeEdge(involvedName).map(_.addExpressions(Seq(filter)))
    if (_edge.isDefined) {
      g.updateEdge(_edge.get)
      return FilterExpression.empty
    }
    // 3. return the filter expression if the filter is not involved in the graph pattern.
    FilterExpression(Map(Set(involvedName) -> Seq(filter)))
  }
}
