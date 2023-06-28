package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical._
import org.grapheco.lynx.runner.{EQUAL, ExecutionContext, IN, PropOp}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.lynx.types.structural.LynxPropertyKey
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.InputPosition

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * rule to PUSH the nodes or relationships' property and label in PPTFilter to NodePattern or RelationshipPattern
 * LEAVE other expressions or operations in PPTFilter.
 * like:
 * PPTFilter(where node.age>10 or node.age<5 and n.label='xxx')           PPTFilter(where node.age>10 or node.age<5)
 * ||                                                           ===>      ||
 * NodePattern(n)                                                         NodePattern(n: label='xxx')
 */
object PPTFilterPushDownRule extends PhysicalPlanOptimizerRule {
  override def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode = optimizeBottomUp(plan, {
    case pnode: PPTNode => {
      pnode.children match {
        case Seq(pf@PPTFilter(exprs)) => {
          val res = pptFilterPushDownRule(pf, pnode, ppc)
          if (res._2) pnode.withChildren(res._1)
          else pnode
        }
        case Seq(pj@PPTJoin(filterExpr, isSingleMatch, joinType)) => {
          val newPPT = pptJoinPushDown(pj, ppc)
          pnode.withChildren(Seq(newPPT))
        }
        case _ => pnode
      }
    }
  })

  def pptJoinPushDown(pj: PPTJoin, ppc: PhysicalPlannerContext): PPTNode = {
    val res = pj.children.map {
      case pf@PPTFilter(expr) => {
        val res = pptFilterPushDownRule(pf, pj, ppc)
        if (res._2) res._1.head
        else pf
      }
      case pjj@PPTJoin(filterExpr, isSingleMatch, joinType) => pptJoinPushDown(pjj, ppc)
      case f => f
    }
    pj.withChildren(res)
  }

  def pptFilterThenJoinPushDown(propMap: Map[String, Option[Expression]],
                                labelMap: Map[String, Seq[LabelName]],
                                pj: PPTJoin, ppc: PhysicalPlannerContext): PPTNode = {
    val res = pj.children.map {
      case pn@PPTNodeScan(pattern) => PPTNodeScan(getNewNodePattern(pattern, labelMap, propMap))(ppc)
      case pr@PPTRelationshipScan(rel, leftNode, rightNode) =>
        PPTRelationshipScan(rel, getNewNodePattern(leftNode, labelMap, propMap), getNewNodePattern(rightNode, labelMap, propMap))(ppc)
      case pjj@PPTJoin(filterExpr, isSingleMatch, joinType) => pptFilterThenJoinPushDown(propMap, labelMap, pjj, ppc)
      case f => f
    }
    pj.withChildren(res)
  }

  def pptFilterThenJoin(parent: PPTFilter, pj: PPTJoin, ppc: PhysicalPlannerContext): (Seq[PPTNode], Boolean) = {
    val (labelMap, propertyMap, notPushDown) = extractFromFilterExpression(parent.expr)

    val res = pptFilterThenJoinPushDown(propertyMap, labelMap, pj, ppc)

    notPushDown.size match {
      case 0 => (Seq(res), true)
      case 1 => (Seq(PPTFilter(notPushDown.head)(res, ppc)), true)
      case 2 => {
        val expr = Ands(Set(notPushDown: _*))(InputPosition(0, 0, 0))
        (Seq(PPTFilter(expr)(res, ppc)), true)
      }
      case _ => (Seq(res), true)
    }
  }

  def extractFromFilterExpression(expression: Expression): (Map[String, Seq[LabelName]], Map[String, Option[Expression]], Seq[Expression]) = {
    val propertyMap: mutable.Map[String, Option[Expression]] = mutable.Map.empty
    val labelMap: mutable.Map[String, Seq[LabelName]] = mutable.Map.empty
    val notPushDown: ArrayBuffer[Expression] = ArrayBuffer.empty
    val propItems: mutable.Map[String, ArrayBuffer[(PropertyKeyName, Expression)]] = mutable.Map.empty
    val propOpsItems: mutable.Map[String, ArrayBuffer[(PropertyKeyName, Expression)]] = mutable.Map.empty
    val regexPattern: mutable.Map[String, ArrayBuffer[RegexMatch]] = mutable.Map.empty

    extractParamsFromFilterExpression(expression, labelMap, propItems, propOpsItems,regexPattern, notPushDown)
    propItems.foreach {
      case (name, exprs) =>
        exprs.size match {
          case 0 => {}
          case _ => {
            propertyMap += name -> Option(
              ListLiteral(Seq(
                MapExpression(List(exprs: _*))(InputPosition(0, 0, 0))
                ,MapExpression(List(propOpsItems.get(name).get: _*))(InputPosition(0, 0, 0))
              ))(InputPosition(0,0,0))
            )
          }
        }
    }
    (labelMap.toMap, propertyMap.toMap, notPushDown)
  }

  def extractParamsFromFilterExpression(filters: Expression,
                                        labelMap: mutable.Map[String, Seq[LabelName]],
                                        propMap: mutable.Map[String, ArrayBuffer[(PropertyKeyName, Expression)]],
                                        propOpsMap: mutable.Map[String, ArrayBuffer[(PropertyKeyName, Expression)]],
                                        regexPattern: mutable.Map[String, ArrayBuffer[RegexMatch]],
                                        notPushDown: ArrayBuffer[Expression]): Unit = {

    filters match {
      case e@Equals(Property(expr, pkn), rhs) => {
        rhs match {
          // Do not push down the Equals is rhs is a Variable.
          case Variable(v) => notPushDown += e
          case _ => expr match {
            case Variable(name) => {
              if (propMap.contains(name)) propMap(name).append((pkn, rhs))
              else propMap += name -> ArrayBuffer((pkn, rhs))
              if (propOpsMap.contains(name)) propOpsMap(name).append((pkn, StringLiteral("EQUAL")(InputPosition(0,0,0))))
              else propOpsMap += name -> ArrayBuffer((pkn, StringLiteral("EQUAL")(InputPosition(0,0,0))))
            }
          }
        }
      }
      case hl@HasLabels(expr, labels) => {
        expr match {
          case Variable(name) => {
            labelMap += name -> labels
          }
        }
      }
      case in@In(Property(expr, pkn), rhs) =>{
        expr match {
          case Variable(name)=>{
            if (propMap.contains(name)) propMap(name).append((pkn,rhs))
            else propMap += name -> ArrayBuffer((pkn,rhs))
            if (propOpsMap.contains(name)) propOpsMap(name).append((pkn, StringLiteral("IN")(InputPosition(0,0,0))))
            else propOpsMap += name -> ArrayBuffer((pkn, StringLiteral("IN")(InputPosition(0,0,0))))

          }
        }
      }
      case a@Ands(andExpress) => andExpress.foreach(exp => extractParamsFromFilterExpression(exp, labelMap, propMap, propOpsMap, regexPattern, notPushDown))
      case other => notPushDown += other
    }
  }

  /**
   *
   * @param pf    the PPTFilter
   * @param pnode the parent of PPTFilter, to rewrite PPTFilter
   * @param ppc   context
   * @return a seq and a flag, flag == true means push-down works
   */
  def pptFilterPushDownRule(pf: PPTFilter, pnode: PPTNode, ppc: PhysicalPlannerContext): (Seq[PPTNode], Boolean) = {
    pf.children match {
      case Seq(pns@PPTNodeScan(pattern)) => {
        val patternAndSet = pushExprToNodePattern(pf.expr, pattern)
        if (patternAndSet._3) {
          if (patternAndSet._2.isEmpty) (Seq(PPTNodeScan(patternAndSet._1)(ppc)), true)
          else (Seq(PPTFilter(patternAndSet._2.head)(PPTNodeScan(patternAndSet._1)(ppc), ppc)), true)
        }
        else (null, false)
      }
      case Seq(prs@PPTRelationshipScan(rel, left, right)) => {
        val patternsAndSet = pushExprToRelationshipPattern(pf.expr, left, right)
        if (patternsAndSet._4) {
          if (patternsAndSet._3.isEmpty)
            (Seq(PPTRelationshipScan(rel, patternsAndSet._1, patternsAndSet._2)(ppc)), true)
          else
            (Seq(PPTFilter(patternsAndSet._3.head)(PPTRelationshipScan(rel, patternsAndSet._1, patternsAndSet._2)(ppc), ppc)), true)
        }
        else (null, false)
      }
      case Seq(pep@PPTExpandPath(rel, right)) => {
        val expandAndSet = expandPathPushDown(pf.expr, right, pep, ppc)
        if (expandAndSet._2.isEmpty) (Seq(expandAndSet._1), true)
        else (Seq(PPTFilter(expandAndSet._2.head)(expandAndSet._1, ppc)), true)
      }
      case Seq(pj@PPTJoin(filterExpr, isSingleMatch, bigTableIndex)) => pptFilterThenJoin(pf, pj, ppc)
      case _ => (null, false)
    }
  }

  def pushExprToNodePattern(expression: Expression, pattern: NodePattern): (NodePattern, Set[Expression], Boolean) = {
    expression match {
      case e@Equals(Property(expr, pkn), rhs) => {
        expr match {
          case Variable(name) => {
            if (pattern.variable.get.name == name) {
              val newPattern = getNewNodePattern(pattern, Map.empty, Map(name -> Option(MapExpression(List((pkn, rhs)))(e.position))))
              (newPattern, Set.empty, true)
            }
            else (pattern, Set(expression), true)
          }
        }
      }
      case hl@HasLabels(expr, labels) => {
        expr match {
          case Variable(name) => {
            val newPattern = getNewNodePattern(pattern, Map(name -> labels), Map.empty)
            (newPattern, Set.empty, true)
          }
        }
      }
      case in@In(lhs, rhs) => {
        (pattern, Set(in), true)
      }
      case andExpr@Ands(exprs) => handleNodeAndsExpression(andExpr, pattern)
      case _ => (pattern, Set.empty, false)
    }
  }

  def pushExprToRelationshipPattern(expression: Expression, left: NodePattern,
                                    right: NodePattern): (NodePattern, NodePattern, Set[Expression], Boolean) = {
    expression match {
      case hl@HasLabels(expr, labels) => {
        expr match {
          case Variable(name) => {
            if (left.variable.get.name == name) {
              val newLeftPattern = getNewNodePattern(left, Map(name -> labels), Map.empty)
              (newLeftPattern, right, Set(), true)
            }
            else if (right.variable.get.name == name) {
              val newRightPattern = getNewNodePattern(right, Map(name -> labels), Map.empty)
              (left, newRightPattern, Set(), true)
            }
            else (left, right, Set(), false)
          }
          case _ => (left, right, Set(), false)
        }
      }
      case e@Equals(Property(map, pkn), rhs) => {
        map match {
          case Variable(name) => {
            if (left.variable.get.name == name) {
              val newLeftPattern = getNewNodePattern(left, Map.empty, Map(name -> Option(MapExpression(List((pkn, rhs)))(InputPosition(0, 0, 0)))))
              (newLeftPattern, right, Set(), true)
            }
            else if (right.variable.get.name == name) {
              val newRightPattern = getNewNodePattern(right, Map.empty, Map(name -> Option(MapExpression(List((pkn, rhs)))(InputPosition(0, 0, 0)))))
              (left, newRightPattern, Set(), true)
            }
            else (left, right, Set(), false)
          }
          case _ => (left, right, Set(), false)
        }
      }
      case andExpr@Ands(expressions) => {
        val (nodeLabels, nodeProperties, otherExpressions) = extractFromFilterExpression(andExpr)

        val leftPattern = getNewNodePattern(left, nodeLabels, nodeProperties)
        val rightPattern = getNewNodePattern(right, nodeLabels, nodeProperties)

        if (otherExpressions.isEmpty) (leftPattern, rightPattern, Set(), true)
        else {
          if (otherExpressions.size > 1) {
            (leftPattern, rightPattern, Set(Ands(otherExpressions.toSet)(InputPosition(0, 0, 0))), true)
          }
          else {
            (leftPattern, rightPattern, Set(otherExpressions.head), true)
          }
        }
      }
      case _ => (left, right, Set(), false)
    }
  }

  def expandPathPushDown(expression: Expression, right: NodePattern,
                         pep: PPTExpandPath, ppc: PhysicalPlannerContext): (PPTNode, Set[Expression]) = {
    val (nodeLabels, nodeProperties, otherExpressions) = extractFromFilterExpression(expression)

    val topExpandPath = bottomUpExpandPath(nodeLabels, nodeProperties, pep, ppc)

    if (otherExpressions.isEmpty) (topExpandPath, Set())
    else {
      if (otherExpressions.size > 1) (topExpandPath, Set(Ands(otherExpressions.toSet)(InputPosition(0, 0, 0))))
      else (topExpandPath, Set(otherExpressions.head))
    }
  }

  def bottomUpExpandPath(nodeLabels: Map[String, Seq[LabelName]], nodeProperties: Map[String, Option[Expression]],
                         pptNode: PPTNode, ppc: PhysicalPlannerContext): PPTNode = {
    pptNode match {
      case e@PPTExpandPath(rel, right) =>
        val newPEP = bottomUpExpandPath(nodeLabels: Map[String, Seq[LabelName]], nodeProperties: Map[String, Option[Expression]], e.children.head, ppc)
        val expandRightPattern = getNewNodePattern(right, nodeLabels, nodeProperties)
        PPTExpandPath(rel, expandRightPattern)(newPEP, ppc)

      case r@PPTRelationshipScan(rel, left, right) => {
        val leftPattern = getNewNodePattern(left, nodeLabels, nodeProperties)
        val rightPattern = getNewNodePattern(right, nodeLabels, nodeProperties)
        PPTRelationshipScan(rel, leftPattern, rightPattern)(ppc)
      }
    }
  }

  def getNewNodePattern(node: NodePattern, nodeLabels: Map[String, Seq[LabelName]],
                        nodeProperties: Map[String, Option[Expression]]): NodePattern = {
    val labelCheck = nodeLabels.get(node.variable.get.name)
    val label = {
      if (labelCheck.isDefined) (labelCheck.get ++ node.labels).distinct
      else node.labels
    }
    val propCheck = nodeProperties.get(node.variable.get.name)
    val props = {
      if (propCheck.isDefined) {
        if (node.properties.isDefined)
          Option(Ands(Set(node.properties.get, propCheck.get.get))(InputPosition(0, 0, 0)))
        else propCheck.get
      }
      else node.properties
    }
    NodePattern(node.variable, label, props, node.baseNode)(node.position)
  }

  def handleNodeAndsExpression(andExpr: Expression, pattern: NodePattern): (NodePattern, Set[Expression], Boolean) = {
    val (pushLabels, propertiesMap, notPushDown) = extractFromFilterExpression(andExpr)

    val newNodePattern = getNewNodePattern(pattern, pushLabels, propertiesMap)

    if (notPushDown.size > 1) (newNodePattern, Set(Ands(notPushDown.toSet)(InputPosition(0, 0, 0))), true)
    else (newNodePattern, notPushDown.toSet, true)
  }
}