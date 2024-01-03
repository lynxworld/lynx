package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.dataframe.{InnerJoin, LeftJoin, OuterJoin}
import org.grapheco.lynx.logical.planner.{LogicalTranslator, translators}
import org.grapheco.lynx.logical.plans.{LogicalAndThen, LogicalCross, LogicalJoin, LogicalPatternMatch, LogicalPlan, LogicalShortestPaths, LogicalUnwind, LogicalWith}
import org.grapheco.lynx.logical.{LogicalPlannerContext, plans}
import org.opencypher.v9_0.ast.{AliasedReturnItem, Match, ReturnItems, Where}
import org.opencypher.v9_0.expressions._

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
// TODO do not join any more
case class MatchTranslator(m: Match) extends LogicalTranslator {
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    //run match TODO OptionalMatch, FIXME: JOIN
    val Match(optional, Pattern(patternParts: Seq[PatternPart]), hints, where: Option[Where]) = m
    val parts = patternParts.map(part => matchPatternPart(optional, part, variableName = None))
    //TODO with variable name how combine?
    val combined = combinePatternMatch(parts.collect{case p: LogicalPatternMatch => p}) ++ parts.filterNot(_.isInstanceOf[LogicalPatternMatch])
    val matched = combined.drop(1).foldLeft(combined.head)(
      (a, b) =>
        if (a == b) LogicalJoin(true, InnerJoin)(a, b)
//        else plans.LogicalJoin(true, OuterJoin)(a, b)
        else LogicalCross()(a, b)
    )
    //    val matched = parts.drop(1).foldLeft(parts.head)((a,b) => LPTJoin(true, InnerJoin)(a, b))
    val filtered = WhereTranslator(where).translate(Some(matched))

    in match {
      case None => filtered
      case Some(w:LogicalWith) => LogicalAndThen()(w,filtered)
      case Some(w:LogicalUnwind) => LogicalAndThen()(w,filtered)
      case Some(a:LogicalAndThen) => LogicalAndThen()(a, filtered)
      case Some(left) => plans.LogicalJoin(false, if (optional) LeftJoin else InnerJoin)(left, filtered) // danger!
    }
  }
  /*
  combine pattern, eg:
  (a)-[]->(b), (b)-[]->(c)  ==> (a) -[]->(b) -[]->(c)
   */
  private def combinePatternMatch(patterns: Seq[LogicalPatternMatch]): Seq[LogicalPatternMatch] = {
    if (patterns.size == 1) return patterns
    val _p = new ArrayBuffer[LogicalPatternMatch]() ++ patterns
    _p.foldLeft(Seq.empty[LogicalPatternMatch]) { (left, right) => left.lastOption match {
      case Some(LogicalPatternMatch(o,n,h,c))
        if c.lastOption.map(_._2).getOrElse(h).variable == right.headNode.variable
        => left.dropRight(1):+LogicalPatternMatch(o,n,h,c++right.chain)
      case None => Seq(right)
      case _ => left:+right
    }}
//    for (i <- _p.indices) {
//      val tail = _p(i).chain.lastOption.map(_._2).getOrElse(_p(i).headNode)
//      for (j <- i until _p.size){
//        val head = _p(j).headNode
//        if (tail.variable.isDefined && tail.variable.map(_.name).equals(head.variable.map(_.name))){
//          _p(i) = _p(i) match {
//            case LogicalPatternMatch(o,v,h,c) => LogicalPatternMatch(o,v,h, c ++: _p(j).chain)
//          }
//          _p.remove(j)
//        }
//      }
//    }
  }

  @tailrec
  private def matchPatternPart(optional: Boolean, patternPart: PatternPart, variableName: Option[String]): LogicalPlan = {
    patternPart match {
      case EveryPath(element: PatternElement) =>
        val mp = matchPattern(element)
        LogicalPatternMatch(optional, variableName, mp._1, mp._2)
      case ShortestPaths(element, single) =>
        val mp = matchPattern(element)
        LogicalShortestPaths(mp._1, mp._2, single, variableName.getOrElse("UNNAME"))
      case NamedPatternPart(variable: Variable, patternPart: AnonymousPatternPart) =>
        matchPatternPart(optional, patternPart, Some(variable.name))
    }
  }

  private def matchPattern(element: PatternElement): (NodePattern, Seq[(RelationshipPattern, NodePattern)]) = {
    element match {
      //match (m:label1)
      case np: NodePattern => (np, Seq.empty)

      //match ()-[]->()
      case rc@RelationshipChain(
      leftNode: NodePattern,
      relationship: RelationshipPattern,
      rightNode: NodePattern) => (leftNode, Seq(relationship -> rightNode))

      //match ()-[]->()-...-[r:type]->(n:label2)
      case rc@RelationshipChain(
      leftChain: RelationshipChain,
      relationship: RelationshipPattern,
      rightNode: NodePattern) => {
        val mp = matchPattern(leftChain)
        (mp._1, mp._2 :+ (relationship -> rightNode))
      }
    }
  }

}
