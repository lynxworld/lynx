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
          pf.children match {
            case Seq(pns@PPTNodeScan(pattern)) => {
              exprs match {
                case e@Equals(Property(map, pkn), rhs) => {
                  val pushProperties: Option[Expression] = Option(MapExpression(List((pkn, rhs)))(e.position))
                  val newPattern = NodePattern(pattern.variable, pattern.labels, pushProperties, pattern.baseNode)(pattern.position)
                  pnode.withChildren(Seq(PPTNodeScan(newPattern)(ppc)))
                }
                case hl@HasLabels(expression, labels) => {
                  val newPattern = NodePattern(pattern.variable, labels, pattern.properties, pattern.baseNode)(pattern.position)
                  pnode.withChildren(Seq(PPTNodeScan(newPattern)(ppc)))
                }
                case a@Ands(exprs) => {
                  val result = handleNodeAndsExpression(exprs, pattern, a.position) // return (NodePattern,otherSituation)
                  if (result._2.isEmpty) pnode.withChildren(Seq(PPTNodeScan(result._1)(ppc)))
                  else pnode.withChildren(Seq(PPTFilter(result._2.head)(PPTNodeScan(result._1)(ppc), ppc)))
                }
                case _ => pnode
              }
            }
            case Seq(prs@PPTRelationshipScan(rel, left, right)) => {
              exprs match {
                case hl@HasLabels(expression, labels) => {
                  expression match {
                    case Variable(name) => {
                      if (left.variable.get.name == name) {
                        val newPattern = NodePattern(left.variable, labels, left.properties, left.baseNode)(left.position)
                        pnode.withChildren(Seq(PPTRelationshipScan(rel, newPattern, right)(ppc)))
                      }
                      else {
                        val newPattern = NodePattern(right.variable, labels, right.properties, right.baseNode)(right.position)
                        pnode.withChildren(Seq(PPTRelationshipScan(rel, left, newPattern)(ppc)))
                      }
                    }
                  }
                }
                case e@Equals(Property(map, pkn), rhs) => {
                  map match {
                    case Variable(name) => {
                      if (left.variable.get.name == name) {
                        val pushProperties: Option[Expression] = Option(MapExpression(List((pkn, rhs)))(left.position))
                        val newPattern = NodePattern(left.variable, left.labels, pushProperties, left.baseNode)(left.position)
                        pnode.withChildren(Seq(PPTRelationshipScan(rel, newPattern, right)(ppc)))
                      }
                      else {
                        val pushProperties: Option[Expression] = Option(MapExpression(List((pkn, rhs)))(right.position))
                        val newPattern = NodePattern(right.variable, right.labels, pushProperties, right.baseNode)(right.position)
                        pnode.withChildren(Seq(PPTRelationshipScan(rel, left, newPattern)(ppc)))
                      }
                    }
                  }
                }
                case a@Ands(expressions) => {
                  val nodeLabels = mutable.Map[String, Seq[LabelName]]()
                  val nodeProperties = mutable.Map[String, Option[Expression]]()
                  val otherSituations = mutable.Set[Expression]()

                  expressions.foreach {
                    case hl@HasLabels(expression, labels) =>
                      nodeLabels += expression.asInstanceOf[Variable].name -> labels

                    case eq@Equals(Property(expression, propertyKey), value) =>
                      nodeProperties += expression.asInstanceOf[Variable].name -> Option(MapExpression(List((propertyKey, value)))(eq.position))

                    case other => otherSituations.add(other)
                  }

                  val leftPattern = getNewNodePattern(left, nodeLabels.toMap, nodeProperties.toMap)
                  val rightPattern = getNewNodePattern(right, nodeLabels.toMap, nodeProperties.toMap)

                  if (otherSituations.isEmpty) pnode.withChildren(Seq(PPTRelationshipScan(rel, leftPattern, rightPattern)(ppc)))
                  else {
                    if (otherSituations.size > 1) pnode.withChildren(Seq(PPTFilter(Ands(otherSituations.toSet)(a.position))(PPTRelationshipScan(rel, leftPattern, rightPattern)(ppc), ppc)))

                    else pnode.withChildren(Seq(PPTFilter(otherSituations.head)(PPTRelationshipScan(rel, leftPattern, rightPattern)(ppc), ppc)))
                  }
                }
                case _ => pnode
              }
            }
            case Seq(pep@PPTExpandPath(rel, right)) => {

              val nodeLabels = mutable.Map[String, Seq[LabelName]]()
              val nodeProperties = mutable.Map[String, Option[Expression]]()
              val otherExceptions = mutable.Set[Expression]()
              var andsPosition: InputPosition = null

              exprs match {
                case a@Ands(exprs2) => {
                  andsPosition = a.position
                  exprs2.foreach {
                    case h@HasLabels(expression, labels) =>
                      nodeLabels += expression.asInstanceOf[Variable].name -> labels

                    case eq@Equals(Property(expression, propertyKey), value) =>
                      nodeProperties += expression.asInstanceOf[Variable].name -> Option(MapExpression(List((propertyKey, value)))(eq.position))

                    case other => otherExceptions.add(other)
                  }
                }
                case other => otherExceptions.add(other)
              }

              val topExpandPath = bottomUpExpandPath(nodeLabels.toMap, nodeProperties.toMap, pep, ppc)

              if (otherExceptions.isEmpty) {
                pnode.withChildren(Seq(topExpandPath))
              }
              else {
                if (otherExceptions.size > 1) {
                  pnode.withChildren(Seq(PPTFilter(Ands(otherExceptions.toSet)(andsPosition))(topExpandPath, ppc)))
                }
                else pnode.withChildren(Seq(PPTFilter(otherExceptions.head)(topExpandPath, ppc)))
              }
            }
            case _ => pnode
          }
        }
        case _ => pnode
      }
    }
  })

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

  def getNewNodePattern(node: NodePattern, nodeLabels: Map[String, Seq[LabelName]], nodeProperties: Map[String, Option[Expression]]): NodePattern = {
    val label = nodeLabels.getOrElse(node.variable.get.name, node.labels)
    val property = nodeProperties.getOrElse(node.variable.get.name, node.properties)
    NodePattern(node.variable, label, property, node.baseNode)(node.position)
  }

  def handleNodeAndsExpression(exprs: Set[Expression], pattern: NodePattern, andsPosition: InputPosition): (NodePattern, Set[Expression]) = {
    var properties = pattern.properties

    val otherSituations: mutable.Set[Expression] = mutable.Set.empty
    var pushLabels: Seq[LabelName] = pattern.labels
    var newNodePattern: NodePattern = null
    exprs.foreach {
      case e@Equals(Property(map, pkn), rhs) => properties = Option(MapExpression(List((pkn, rhs)))(e.position))
      case hl@HasLabels(expression, labels) => pushLabels = labels
      case other => otherSituations.add(other)
    }

    newNodePattern = NodePattern(pattern.variable, pushLabels, properties, pattern.baseNode)(pattern.position)

    if (otherSituations.size > 1) (newNodePattern, Set(Ands(otherSituations.toSet)(andsPosition)))

    else (newNodePattern, otherSituations.toSet)
  }
}