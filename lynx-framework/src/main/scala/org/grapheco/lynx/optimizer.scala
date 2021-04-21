package org.grapheco.lynx

import org.opencypher.v9_0.ast.AliasedReturnItem
import org.opencypher.v9_0.expressions.{Ands, Equals, Expression, HasLabels, LabelName, MapExpression, NodePattern, Not, Ors, Property, PropertyKeyName, Variable}
import org.opencypher.v9_0.util.InputPosition

import scala.collection.mutable

trait PhysicalPlanOptimizer {
  def optimize(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode
}

trait PhysicalPlanOptimizerRule {
  def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode

  def optimizeBottomUp(node: PPTNode, ops: PartialFunction[PPTNode, PPTNode]*): PPTNode = {
    val childrenOptimized = node.withChildren(node.children.map(child => optimizeBottomUp(child, ops: _*)))
    ops.foldLeft(childrenOptimized) {
      (optimized, op) =>
        op.lift(optimized).getOrElse(optimized)
    }
  }
}

class DefaultPhysicalPlanOptimizer(runnerContext: CypherRunnerContext) extends PhysicalPlanOptimizer {
  val rules = Seq[PhysicalPlanOptimizerRule](
    RemoveNullProject,
    NodeFilterPushDownRule
  )

  def optimize(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode = {
    rules.foldLeft(plan)((optimized, rule) => rule.apply(optimized, ppc))
  }
}

object RemoveNullProject extends PhysicalPlanOptimizerRule {

  override def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode = optimizeBottomUp(plan,
    {
      case pnode: PPTNode =>
        pnode.children match {
          case Seq(p@PPTProject(ri)) if ri.items.forall {
            case AliasedReturnItem(expression, variable) => expression == variable
          } => pnode.withChildren(pnode.children.filterNot(_ eq p) ++ p.children)

          case _ => pnode
        }
    }
  )
}

object NodeFilterPushDownRule extends PhysicalPlanOptimizerRule {
  override def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode = optimizeBottomUp(plan, {
    case pnode: PPTNode => {
      pnode.children match {
        case Seq(pf@PPTFilter(exprs)) => {
          val res = pptFilterPushDownRule(exprs, pf, pnode, ppc)
          if (res._2) pnode.withChildren(res._1)
          else pnode
        }
        case Seq(pj@PPTJoin()) => {
          val newPPT = pptJoinChildrenMap(pj, ppc)
          pnode.withChildren(Seq(newPPT))
        }
        case _ => pnode
      }
    }
  })

  def pptJoinChildrenMap(pj: PPTJoin, ppc: PhysicalPlannerContext): PPTNode = {
    val res = pj.children.map {
      case pf@PPTFilter(expr) => {
        val res = pptFilterPushDownRule(expr, pf, pj, ppc)
        if (res._2) res._1.head
        else pf
      }
      case pjj@PPTJoin() => pptJoinChildrenMap(pjj, ppc)
      case f => f
    }
    pj.withChildren(res)
  }

  def pptFilterPushDownRule(exprs: Expression, pf: PPTFilter, pnode: PPTNode, ppc: PhysicalPlannerContext): (Seq[PPTNode], Boolean) = {
    pf.children match {
      case Seq(pns@PPTNodeScan(pattern)) => {
        val patternAndSet = nodeScanPushDown(exprs, pattern)
        if (patternAndSet._3) {
          if (patternAndSet._2.isEmpty) (Seq(PPTNodeScan(patternAndSet._1)(ppc)), true)
          else (Seq(PPTFilter(patternAndSet._2.head)(PPTNodeScan(patternAndSet._1)(ppc), ppc)), true)
        }
        else (null, false)
      }
      case Seq(prs@PPTRelationshipScan(rel, left, right)) => {
        val patternsAndSet = relationshipScanPushDown(exprs, left, right)
        if (patternsAndSet._4) {
          if (patternsAndSet._3.isEmpty)
            (Seq(PPTRelationshipScan(rel, patternsAndSet._1, patternsAndSet._2)(ppc)), true)
          else
            (Seq(PPTFilter(patternsAndSet._3.head)(PPTRelationshipScan(rel, patternsAndSet._1, patternsAndSet._2)(ppc), ppc)), true)
        }
        else (null, false)
      }
      case Seq(pep@PPTExpandPath(rel, right)) => {
        val expandAndSet = expandPathPushDown(exprs, right, pep, ppc)
        if (expandAndSet._2.isEmpty) (Seq(expandAndSet._1), true)
        else (Seq(PPTFilter(expandAndSet._2.head)(expandAndSet._1, ppc)), true)
      }
      case _ => (null, false)
    }
  }

  def nodeScanPushDown(expression: Expression, pattern: NodePattern): (NodePattern, Set[Expression], Boolean) = {
    expression match {
      case e@Equals(Property(map, pkn), rhs) => {
        val pushProperties: Option[Expression] = Option(MapExpression(List((pkn, rhs)))(e.position))
        val newPattern = NodePattern(pattern.variable, pattern.labels, pushProperties, pattern.baseNode)(pattern.position)
        (newPattern, Set.empty, true)
      }
      case hl@HasLabels(expression, labels) => {
        val newPattern = NodePattern(pattern.variable, labels, pattern.properties, pattern.baseNode)(pattern.position)
        (newPattern, Set.empty, true)
      }
      case a@Ands(exprs) => handleNodeAndsExpression(exprs, pattern, a.position)

      case _ => (pattern, Set.empty, false)
    }
  }

  def relationshipScanPushDown(expression: Expression, left: NodePattern,
                               right: NodePattern): (NodePattern, NodePattern, Set[Expression], Boolean) = {
    expression match {
      case hl@HasLabels(expr, labels) => {
        expr match {
          case Variable(name) => {
            if (left.variable.get.name == name) {
              val newLeftPattern = NodePattern(left.variable, labels, left.properties, left.baseNode)(left.position)
              (newLeftPattern, right, Set(), true)
            }
            else {
              val newRightPattern = NodePattern(right.variable, labels, right.properties, right.baseNode)(right.position)
              (left, newRightPattern, Set(), true)
            }
          }
          case _ => (left, right, Set(), false)
        }
      }
      case e@Equals(Property(map, pkn), rhs) => {
        map match {
          case Variable(name) => {
            if (left.variable.get.name == name) {
              val pushProperties: Option[Expression] = Option(MapExpression(List((pkn, rhs)))(left.position))
              val newLeftPattern = NodePattern(left.variable, left.labels, pushProperties, left.baseNode)(left.position)
              (newLeftPattern, right, Set(), true)
            }
            else {
              val pushProperties: Option[Expression] = Option(MapExpression(List((pkn, rhs)))(right.position))
              val newRightPattern = NodePattern(right.variable, right.labels, pushProperties, right.baseNode)(right.position)
              (left, newRightPattern, Set(), true)
            }
          }
          case _ => (left, right, Set(), false)
        }
      }
      case a@Ands(expressions) => {
        val nodeLabels = mutable.Map[String, Seq[LabelName]]()
        val nodeProperties = mutable.Map[String, Option[Expression]]()
        val otherExpressions = mutable.Set[Expression]()

        expressions.foreach {
          case hl@HasLabels(expr, labels) =>
            nodeLabels += expr.asInstanceOf[Variable].name -> labels

          case eq@Equals(Property(expr, propertyKey), value) =>
            nodeProperties += expr.asInstanceOf[Variable].name -> Option(MapExpression(List((propertyKey, value)))(eq.position))

          case other => otherExpressions.add(other)
        }

        val leftPattern = getNewNodePattern(left, nodeLabels.toMap, nodeProperties.toMap)
        val rightPattern = getNewNodePattern(right, nodeLabels.toMap, nodeProperties.toMap)

        if (otherExpressions.isEmpty) (leftPattern, rightPattern, Set(), true)
        else {
          if (otherExpressions.size > 1) {
            (leftPattern, rightPattern, Set(Ands(otherExpressions.toSet)(a.position)), true)
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
    val nodeLabels = mutable.Map[String, Seq[LabelName]]()
    val nodeProperties = mutable.Map[String, Option[Expression]]()
    val otherExpressions = mutable.Set[Expression]()
    var andsPosition: InputPosition = null

    expression match {
      case a@Ands(exprs) => {
        andsPosition = a.position
        exprs.foreach {
          case h@HasLabels(expression, labels) =>
            nodeLabels += expression.asInstanceOf[Variable].name -> labels

          case eq@Equals(Property(expression, propertyKey), value) =>
            nodeProperties += expression.asInstanceOf[Variable].name -> Option(MapExpression(List((propertyKey, value)))(eq.position))

          case other => otherExpressions.add(other)
        }
      }
      case other => otherExpressions.add(other)
    }

    val topExpandPath = bottomUpExpandPath(nodeLabels.toMap, nodeProperties.toMap, pep, ppc)

    if (otherExpressions.isEmpty) (topExpandPath, Set())
    else {
      if (otherExpressions.size > 1) (topExpandPath, Set(Ands(otherExpressions.toSet)(andsPosition)))
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
    val label = nodeLabels.getOrElse(node.variable.get.name, node.labels)
    val property = nodeProperties.getOrElse(node.variable.get.name, node.properties)
    NodePattern(node.variable, label, property, node.baseNode)(node.position)
  }

  def handleNodeAndsExpression(exprs: Set[Expression], pattern: NodePattern,
                               andsPosition: InputPosition): (NodePattern, Set[Expression], Boolean) = {
    var properties = pattern.properties
    val otherExpressions: mutable.Set[Expression] = mutable.Set.empty
    var pushLabels: Seq[LabelName] = pattern.labels
    var newNodePattern: NodePattern = null
    exprs.foreach {
      case e@Equals(Property(map, pkn), rhs) => properties = Option(MapExpression(List((pkn, rhs)))(e.position))
      case hl@HasLabels(expression, labels) => pushLabels = labels
      case other => otherExpressions.add(other)
    }
    newNodePattern = NodePattern(pattern.variable, pushLabels, properties, pattern.baseNode)(pattern.position)

    if (otherExpressions.size > 1) (newNodePattern, Set(Ands(otherExpressions.toSet)(andsPosition)), true)
    else (newNodePattern, otherExpressions.toSet, true)
  }
}