package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical._
import org.opencypher.v9_0.expressions._

import scala.collection.mutable

/**
 * rule to check is there any reference property between two match clause
 * if there is any reference property, extract it to PPTJoin().
 * like:
 * PPTJoin()                                             PPTJoin(m.city == n.city)
 * ||                                    ====>           ||
 * match (n:person{name:'alex'})                         match (n:person{name:'alex'})
 * match (m:person where m.city=n.city})                 match (m:person)
 */
object JoinReferenceRule extends PhysicalPlanOptimizerRule {
  def checkNodeReference(pattern: NodePattern): (Seq[((LogicalVariable, PropertyKeyName), Expression)], Option[NodePattern]) = {
    val variable = pattern.variable
    val properties = pattern.properties

    if (properties.isDefined && properties.get.isInstanceOf[MapExpression]) {
      val MapExpression(items) = properties.get.asInstanceOf[MapExpression]
      // filter1 is the reference filter condition
      val filter1 = items.filterNot(item => item._2.isInstanceOf[Literal])
      val filter2 = items.filter(item => item._2.isInstanceOf[Literal])
      val newPattern = {
        if (filter2.nonEmpty) {
          NodePattern(variable, pattern.labels, Option(MapExpression(filter2)(properties.get.position)), pattern.baseNode)(pattern.position)
        }
        else NodePattern(variable, pattern.labels, None, pattern.baseNode)(pattern.position)
      }
      if (filter1.nonEmpty) (filter1.map(f => (variable.get, f._1) -> f._2), Some(newPattern))
      else (Seq.empty, None)
    }
    else (Seq.empty, None)
  }

  def checkFilterReference(pptFilter: PPTFilter): (Seq[Expression], Option[PPTFilter]) = {
    pptFilter.expr match {
      case in@In(lhs, rhs) => {
        (Seq(in), Some(pptFilter))
      }
    }
  }

  def checkExpandPath(pe: PPTExpandPath, ppc: PhysicalPlannerContext): (PPTNode, Seq[((LogicalVariable, PropertyKeyName), Expression)]) = {
    pe.children match {
      case Seq(pr@PPTRelationshipScan(rel, leftPattern, rightPattern)) => {
        val leftChecked = checkNodeReference(leftPattern)
        val rightChecked = checkNodeReference(rightPattern)

        val res = (leftChecked._2, rightChecked._2) match {
          case (None, None) => pr
          case (value1, None) => PPTRelationshipScan(rel, leftChecked._2.get, rightPattern)(ppc)
          case (None, value2) => PPTRelationshipScan(rel, leftPattern, rightChecked._2.get)(ppc)
          case (value1, value2) => PPTRelationshipScan(rel, leftChecked._2.get, rightChecked._2.get)(ppc)
        }

        (pe.withChildren(Seq(res)), leftChecked._1 ++ rightChecked._1)
      }
      case Seq(pe2@PPTExpandPath(rel, rightPattern)) => {
        val res = checkExpandPath(pe2, ppc)
        val rightChecked = checkNodeReference(rightPattern)

        val newPPTExpandPath = {
          if (rightChecked._2.nonEmpty) {
            PPTExpandPath(rel, rightChecked._2.get)(res._1, ppc)
          }
          else pe2.withChildren(Seq(res._1))
        }

        (newPPTExpandPath, res._2 ++ rightChecked._1)
      }
    }
  }

  def joinReferenceRule(table: PPTNode, ppc: PhysicalPlannerContext): (PPTNode, Seq[((LogicalVariable, PropertyKeyName), Expression)], Seq[Expression]) = {
    var referenceProperty = Seq[((LogicalVariable, PropertyKeyName), Expression)]()
    var referenceExpression = Seq[Expression]()
    val newTable = table match {
      case pw@PPTWith() => pw
      case ps@PPTNodeScan(pattern) => {
        val checked = checkNodeReference(pattern)
        referenceProperty = referenceProperty ++ checked._1
        if (checked._2.isDefined) {
          PPTNodeScan(checked._2.get)(ppc)
        }
        else ps
      }
      case pr@PPTRelationshipScan(rel, leftPattern, rightPattern) => {
        val leftChecked = checkNodeReference(leftPattern)
        val rightChecked = checkNodeReference(rightPattern)
        referenceProperty ++= leftChecked._1
        referenceProperty ++= rightChecked._1

        (leftChecked._2, rightChecked._2) match {
          case (None, None) => table
          case (value1, None) => PPTRelationshipScan(rel, leftChecked._2.get, rightPattern)(ppc)
          case (None, value2) => PPTRelationshipScan(rel, leftPattern, rightChecked._2.get)(ppc)
          case (value1, value2) => PPTRelationshipScan(rel, leftChecked._2.get, rightChecked._2.get)(ppc)
        }
      }
      case shortestPaths: PPTShortestPath => {
        val PPTShortestPath(rel: RelationshipPattern, leftPattern: NodePattern, rightPattern: NodePattern, single: Boolean, resName: String) = shortestPaths
        val leftChecked = checkNodeReference(leftPattern)
        val rightChecked = checkNodeReference(rightPattern)
        referenceProperty ++= leftChecked._1
        referenceProperty ++= rightChecked._1

        (leftChecked._2, rightChecked._2) match {
          case (None, None) => table
          case (value1, None) => PPTShortestPath(rel, leftChecked._2.get, rightPattern, single, resName)(ppc)
          case (None, value2) => PPTShortestPath(rel, leftPattern, rightChecked._2.get, single, resName)(ppc)
          case (value1, value2) => PPTShortestPath(rel, leftChecked._2.get, rightChecked._2.get, single, resName)(ppc)
        }
      }
      case pe@PPTExpandPath(rel, rightPattern) => {
        val res = checkExpandPath(pe, ppc)
        referenceProperty ++= res._2
        res._1
      }
      case pm@PPTMerge(mergeSchema, mergeOps, onMatch, onCreate) => {
        pm.children.head match {
          case pj2@PPTJoin(filterExpr, isSingleMatch, joinType) => {
            pm.withChildren(Seq(joinRecursion(pj2, ppc, isSingleMatch)))
          }
          case _ => {
            pm
          }
        }
      }
      case ps@PPTSelect(columns) => {
        ps.children.head match {
          case pj2@PPTJoin(filterExpr, isSingleMatch, joinType) => {
            ps.withChildren(Seq(joinRecursion(pj2, ppc, isSingleMatch)))
          }
          case _ => {
            ps
          }
        }
      }
      case pc@PPTCreateUnit(items) => pc

      case pf@PPTFilter(expr) => {
        val (children, refProp, otherExprs) = joinReferenceRule(pf.children.head, ppc)
        referenceProperty ++= refProp
        referenceExpression ++= Seq(expr)
        children
      }
      case pj1@PPTJoin(filterExpr, isSingleMatch, joinType) => {
        joinRecursion(pj1, ppc, isSingleMatch)
      }
      case pu@PPTUnwind(expr, variable) => pu
      case pd@PPTDistinct() => pd
    }
    (newTable, referenceProperty, referenceExpression)
  }

  def joinRecursion(pj: PPTJoin, ppc: PhysicalPlannerContext, isSingleMatch: Boolean): PPTNode = {
    val (operator1, referenceProps1, otherExprs1) = joinReferenceRule(pj.children.head, ppc)
    val (operator2, referenceProps2, otherExprs2) = joinReferenceRule(pj.children.last, ppc)
    val table1 = operator1
    val table2 = operator2

    val referenceProps: Seq[((LogicalVariable, PropertyKeyName), Expression)] = referenceProps1 ++ referenceProps2
    val referenceExprs: Seq[Expression] = otherExprs1 ++ otherExprs2

    if (referenceProps.nonEmpty || referenceExprs.nonEmpty) {
      val filterExpressions: Seq[Expression] = referenceProps.map{
        case ((logicalVariable, propertyKeyName), expression) => Equals(Property(logicalVariable, propertyKeyName)(logicalVariable.position), expression)(expression.position)
      } ++ referenceExprs

      val _position = {
        if (referenceExprs.nonEmpty) referenceExprs.head.position
        else referenceProps.head._1._1.position
      }

      if (filterExpressions.length == 1) PPTJoin(Option(filterExpressions.head), isSingleMatch, pj.joinType)(table1, table2, ppc)
      else PPTJoin(Option(Ands(filterExpressions.toSet)(_position)), isSingleMatch, pj.joinType)(table1, table2, ppc)
    } else pj
  }

  override def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode = optimizeBottomUp(plan,
    {
      case pnode: PPTNode =>
        pnode.children match {
          case Seq(pj@PPTJoin(filterExpr, isSingleMatch, bigTableIndex)) => {
            val res1 = joinRecursion(pj, ppc, isSingleMatch)
            pnode.withChildren(Seq(res1))
          }
          case _ => pnode
        }
    }
  )
}
