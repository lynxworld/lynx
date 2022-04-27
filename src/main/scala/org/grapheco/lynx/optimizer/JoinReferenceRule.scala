package org.grapheco.lynx.optimizer

import org.grapheco.lynx.context.PhysicalPlannerContext
import org.grapheco.lynx._
import org.grapheco.lynx.physical.{PPTCreateUnit, PPTDistinct, PPTExpandPath, PPTFilter, PPTJoin, PPTMerge, PPTNode, PPTNodeScan, PPTRelationshipScan, PPTSelect, PPTUnwind}
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

    if (properties.isDefined) {
      val MapExpression(items) = properties.get.asInstanceOf[MapExpression]
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

  // TODO rewrite
  def joinReferenceRule(table: PPTNode, ppc: PhysicalPlannerContext): (PPTNode, Seq[((LogicalVariable, PropertyKeyName), Expression)]) = {
    var referenceProperty = Seq[((LogicalVariable, PropertyKeyName), Expression)]()
    val newTable = table match {
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
      case pe@PPTExpandPath(rel, rightPattern) => {
        val res = checkExpandPath(pe, ppc)
        referenceProperty ++= res._2
        res._1
      }
      case pm@PPTMerge(mergeSchema, mergeOps) => {
        pm.children.head match {
          case pj2@PPTJoin(filterExpr, isSingleMatch, bigTableIndex) => {
            pm.withChildren(Seq(joinRecursion(pj2, ppc, isSingleMatch)))
          }
          case _ => {
            pm
          }
        }
      }
      case ps@PPTSelect(columns) => {
        ps.children.head match {
          case pj2@PPTJoin(filterExpr, isSingleMatch, bigTableIndex) => {
            ps.withChildren(Seq(joinRecursion(pj2, ppc, isSingleMatch)))
          }
          case _ => {
            ps
          }
        }
      }
      case pc@PPTCreateUnit(items) => pc

      case pf@PPTFilter(expr) => {
        val (children, refProp) = joinReferenceRule(pf.children.head, ppc)
        referenceProperty ++= refProp
        pf.withChildren(Seq(children))
      }
      case pj1@PPTJoin(filterExpr, isSingleMatch, bigTableIndex) => {
        joinRecursion(pj1, ppc, isSingleMatch)
      }
      case pu@PPTUnwind(expr, variable) => pu
      case pd@PPTDistinct() => pd
    }
    (newTable, referenceProperty)
  }

  // TODO rewrite
  def joinRecursion(pj: PPTJoin, ppc: PhysicalPlannerContext, isSingleMatch: Boolean): PPTNode = {
    var table1 = pj.children.head
    var table2 = pj.children.last
    var referenceProperty = Seq[((LogicalVariable, PropertyKeyName), Expression)]()

    val res1 = joinReferenceRule(table1, ppc)
    val res2 = joinReferenceRule(table2, ppc)
    table1 = res1._1
    table2 = res2._1
    referenceProperty ++= res1._2
    referenceProperty ++= res2._2

    if (referenceProperty.nonEmpty) {
      referenceProperty.length match {
        case 1 => {
          val ksv = referenceProperty.head
          val filter = Equals(Property(ksv._1._1, ksv._1._2)(ksv._1._1.position), ksv._2)(ksv._1._1.position)
          PPTJoin(Option(filter), isSingleMatch)(table1, table2, ppc)
        }
        case _ => {
          val setExprs = mutable.Set[Expression]()
          referenceProperty.foreach(f => {
            val filter = Equals(Property(f._1._1, f._1._2)(f._1._1.position), f._2)(f._1._1.position)
            setExprs.add(filter)
          })
          PPTJoin(Option(Ands(setExprs.toSet)(referenceProperty.head._1._1.position)), isSingleMatch)(table1, table2, ppc)
        }
      }
    }
    else pj
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
