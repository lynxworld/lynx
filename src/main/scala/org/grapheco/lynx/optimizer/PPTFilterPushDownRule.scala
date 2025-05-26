package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.plans.{Cross, Expand, Filter, Join, NodeScan, NodeSeekByID, PhysicalPlan, RelationshipScan, ShortestPath}
import org.grapheco.lynx.physical._
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.InputPosition
import org.grapheco.lynx.procedure.ProcedureExpression

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
  override def apply(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = optimizeBottomUp(plan, {
    case filter: Filter =>{
      val res = pptFilterPushDownRule(filter, ppc)
      if (res._2) res._1.head
      else filter
    }
    case pnode: PhysicalPlan => {
      pnode.children match {
        case Seq(pf@Filter(exprs)) => {
          val res = pptFilterPushDownRule(pf, ppc)
          if (res._2) pnode.withChildren(res._1)
          else pnode
        }
        case Seq(pj@Join(filterExpr, isSingleMatch, joinType)) => {
          val newPPT = pptJoinPushDown(pj, ppc)
          pnode.withChildren(Seq(newPPT))
        }
        case Seq(pc@Cross()) => {
          val newPPT = pptJoinPushDown(pc, ppc)
          pnode.withChildren(Seq(newPPT))
        }
        case _ => pnode
      }
    }
  })

  def pptJoinPushDown(pj: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = {
    val res = pj.children.map {
      case pf@Filter(expr) => {
        val res = pptFilterPushDownRule(pf, ppc)
        if (res._2) res._1.head
        else pf
      }
      case pjj@Join(filterExpr, isSingleMatch, joinType) => pptJoinPushDown(pjj, ppc)
      case f => f
    }
    pj.withChildren(res)
  }

  def pptFilterThenJoinPushDown(propMap: Map[String, Option[Expression]],
                                labelMap: Map[String, Seq[LabelName]],
                                pj: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = {
    val res = pj.children.map {
      case pn@NodeScan(pattern) => plans.NodeScan(getNewNodePattern(pattern, labelMap, propMap))(ppc)
      case path : ShortestPath => {
        val ShortestPath(rel: RelationshipPattern, leftNode: NodePattern, rightNode: NodePattern, single: Boolean, resName: String) = path
        plans.ShortestPath(rel, getNewNodePattern(leftNode, labelMap, propMap), getNewNodePattern(rightNode, labelMap, propMap), single, resName)(ppc)
      }
      case pr@RelationshipScan(rel, leftNode, rightNode) =>
        plans.RelationshipScan(rel, getNewNodePattern(leftNode, labelMap, propMap), getNewNodePattern(rightNode, labelMap, propMap))(ppc)
      case pjj@Join(filterExpr, isSingleMatch, joinType) => pptFilterThenJoinPushDown(propMap, labelMap, pjj, ppc)
      case pcc@Cross() => pptFilterThenJoinPushDown(propMap, labelMap, pcc, ppc)
      case f => f
    }
    pj.withChildren(res)
  }

  def pptFilterThenJoin(parent: Filter, pj: PhysicalPlan, ppc: PhysicalPlannerContext): (Seq[PhysicalPlan], Boolean) = {
    val (labelMap, propertyMap, notPushDown) = extractFromFilterExpression(parent.expr)

    val res = pptFilterThenJoinPushDown(propertyMap, labelMap, pj, ppc)

    notPushDown.size match {
      case 0 => (Seq(res), true)
      case 1 => (Seq(plans.Filter(notPushDown.head)(res, ppc)), true)
      case 2 => {
        val expr = Ands(Set(notPushDown: _*))(InputPosition(0, 0, 0))
        (Seq(plans.Filter(expr)(res, ppc)), true)
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

    extractParamsFromFilterExpression(expression, labelMap, propItems, propOpsItems, regexPattern, notPushDown)

    propItems.foreach {
      case (name, exprs) =>
        exprs.size match {
          case 0 => {}
          case _ => {
            propertyMap += name -> Option(MapExpression(List(exprs: _*))(InputPosition(0, 0, 0)))
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
      case e@Equals(lhs, rhs) => lhs match {
          case Property(expr, pkn) => rhs match {
              // Do not push down the Equals is rhs is a Variable.
              case Variable(v) => notPushDown += e
              case _ => expr match {
                case Variable(name) => updatePropPushDownMap(filters, name, pkn, "EQUAL", rhs, propMap, propOpsMap, notPushDown)
                case _ => notPushDown += e
              }
            }
          case ProcedureExpression(FunctionInvocation(Namespace(List()), FunctionName("id"), false, Vector(Variable(nodeName)))) =>
            updatePropPushDownMap(filters, nodeName, PropertyKeyName("_lynx_sys_id")(InputPosition(0, 0, 0)), "EQUAL", rhs, propMap, propOpsMap, notPushDown)
          case _ => notPushDown += e
        }
      case ne@Not(expr) => {
        expr match {
          case e@Equals(Property(expr, pkn), rhs) => {
            rhs match {
              // Do not push down the Equals is rhs is a Variable.
              case Variable(v) => notPushDown += e
              case _ => expr match {
                case Variable(name) =>
                  updatePropPushDownMap(filters, name, pkn, "NOTEQUALS", rhs, propMap, propOpsMap, notPushDown)
                case _ => notPushDown += e
              }
            }
          }
          case _ => notPushDown += filters // support '<>' operator for now. TODO: expand others expression
        }
      }
      case hl@HasLabels(expr, labels) => {
        expr match {
          case Variable(name) => {
            labelMap += name -> labels
          }
          case _ => notPushDown += filters // TODO: expand others expression
        }

      }
      case greaterThan@GreaterThan(Property(expr, pkn), rhs) => {
        expr match {
          case Variable(name) =>
            if (!labelMap.contains(name)) {
              notPushDown += filters
            } else {// TODO: push down all expressions of same label.
              updatePropPushDownMap(filters, name, pkn, "GreaterThan", rhs, propMap, propOpsMap, notPushDown)
            }
          case _ => notPushDown += filters // TODO: expand others expression
        }
      }
      case greaterThanOrEq@GreaterThanOrEqual(Property(expr, pkn), rhs) => {
        expr match {
          case Variable(name) => {
            if (!labelMap.contains(name)) {
              notPushDown += filters
            } else {
              updatePropPushDownMap(filters, name, pkn, "GreaterThanOrEqual", rhs, propMap, propOpsMap, notPushDown)
            }
          }
          case _ => notPushDown += filters // TODO: expand others expression
        }
      }
      case lessThan@LessThan(Property(expr, pkn), rhs) => {
        expr match {
          case Variable(name) => {
            if (!labelMap.contains(name)) { // fixme: push down all labels
              notPushDown += filters
            } else {
              updatePropPushDownMap(filters, name, pkn, "LessThan", rhs, propMap, propOpsMap, notPushDown)
            }
          }
          case _ => notPushDown += filters // TODO: expand others expression
        }
      }
      case lessThanOrEq@LessThanOrEqual(Property(expr, pkn), rhs) => {
        expr match {
          case Variable(name) => {
            if (!labelMap.contains(name)) {
              notPushDown += filters
            } else {
              updatePropPushDownMap(filters, name, pkn, "LessThanOrEqual", rhs, propMap, propOpsMap, notPushDown)
            }
          }
          case _ => notPushDown += filters // TODO: expand others expression
        }
      }
      case in@In(Property(expr, pkn), rhs) => {
        expr match {
          case Variable(name) => {
            if (!labelMap.contains(name)) {
              notPushDown += filters
            } else {
              updatePropPushDownMap(filters, name, pkn, "IN", rhs, propMap, propOpsMap, notPushDown)
            }
          }
          case _ => notPushDown += filters // TODO: expand others expression
        }
      }
      case contains@Contains(Property(expr, pkn), rhs) => {
        expr match {
          case Variable(name) => {
            if (!labelMap.contains(name)) {
              notPushDown += filters
            } else {
              updatePropPushDownMap(filters, name, pkn, "Contains", rhs, propMap, propOpsMap, notPushDown)
            }
          }
          case _ => notPushDown += filters // TODO: expand others expression
        }
      }
      case a@Ands(andExpress) => andExpress.foreach(exp => extractParamsFromFilterExpression(exp, labelMap, propMap, propOpsMap, regexPattern, notPushDown))
      case other => notPushDown += other
    }
  }

  def patternToPlan(pattern: NodePattern)(ppc: PhysicalPlannerContext): PhysicalPlan = {
    val prop = pattern.properties
    prop match {
      case Some(m:MapExpression) =>
        val id = m.items.find(_._1.name.equals("_lynx_sys_id")).map(_._2)
        if (id.isDefined)
          NodeSeekByID(pattern.variable, id)(ppc)
        else
          plans.NodeScan(pattern)(ppc)
      case _ => plans.NodeScan(pattern)(ppc)
    }
  }

  /**
   *
   * @param pf    the PPTFilter
   * @param pnode the parent of PPTFilter, to rewrite PPTFilter
   * @param ppc   context
   * @return a seq and a flag, flag == true means push-down works
   */
  def pptFilterPushDownRule(pf: Filter, ppc: PhysicalPlannerContext): (Seq[PhysicalPlan], Boolean) = {
    pf.children match {
      case Seq(pns@NodeScan(pattern)) => {
        val patternAndSet =  handleNodeAndsExpression(pf.expr, pattern)
        if (patternAndSet._3) {
          if (patternAndSet._2.isEmpty) (Seq(patternToPlan(patternAndSet._1)(ppc)), true)
          else (Seq(plans.Filter(patternAndSet._2.head)(patternToPlan(patternAndSet._1)(ppc), ppc)), true)
        }
        else (null, false)
      }
      case Seq(prs@RelationshipScan(rel, left, right)) => {
        val patternsAndSet = pushExprToRelationshipPattern(rel, pf.expr, left, right)
        if (patternsAndSet._5) {
          if (patternsAndSet._4.isEmpty)
            (Seq(plans.RelationshipScan(patternsAndSet._1, patternsAndSet._2, patternsAndSet._3)(ppc)), true)
          else
            (Seq(plans.Filter(patternsAndSet._4.head)(plans.RelationshipScan(patternsAndSet._1, patternsAndSet._2, patternsAndSet._3)(ppc), ppc)), true)
        }
        else (null, false)
      }
      case Seq(path: ShortestPath) => {
        val ShortestPath(rel: RelationshipPattern, left: NodePattern, right: NodePattern, single: Boolean, resName: String) = path
        val patternsAndSet = pushExprToRelationshipPattern(rel, pf.expr, left, right)
        if (patternsAndSet._5) {
          if (patternsAndSet._4.isEmpty)
            (Seq(plans.ShortestPath(rel, patternsAndSet._2, patternsAndSet._3, single, resName)(ppc)), true)
          else
            (Seq(plans.Filter(patternsAndSet._4.head)(plans.ShortestPath(rel, patternsAndSet._2, patternsAndSet._2, single, resName)(ppc), ppc)), true)
        }
        else (null, false)
      }
      case Seq(pep@Expand(rel, right)) => {
        val expandAndSet = expandPathPushDown(pf.expr, right, pep, ppc)
        if (expandAndSet._2.isEmpty) (Seq(expandAndSet._1), true)
        else (Seq(plans.Filter(expandAndSet._2.head)(expandAndSet._1, ppc)), true)
      }
      case Seq(pj@Join(filterExpr, isSingleMatch, bigTableIndex)) => pptFilterThenJoin(pf, pj, ppc)
      case Seq(pc@Cross()) => {
        pptFilterThenJoin(pf, pc, ppc)
      }
      case _ => (null, false)
    }
  }

  @deprecated
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

  def pushExprToRelationshipPattern(rel: RelationshipPattern, expression: Expression, left: NodePattern,
                                    right: NodePattern): (RelationshipPattern, NodePattern, NodePattern, Set[Expression], Boolean) = {
    expression match {
      case hl@HasLabels(expr, labels) => {
        expr match {
          case Variable(name) => {
            if (left.variable.get.name == name) {
              val newLeftPattern = getNewNodePattern(left, Map(name -> labels), Map.empty)
              (rel, newLeftPattern, right, Set(), true)
            }
            else if (right.variable.get.name == name) {
              val newRightPattern = getNewNodePattern(right, Map(name -> labels), Map.empty)
              (rel, left, newRightPattern, Set(), true)
            }
            else (rel, left, right, Set(), false)
          }
          case _ => (rel, left, right, Set(), false)
        }
      }
      case e@Equals(Property(map, pkn), rhs) => {
        map match {
          case Variable(name) => {
            if (left.variable.get.name == name) {
              val newLeftPattern = getNewNodePattern(left, Map.empty, Map(name -> Option(MapExpression(List((pkn, rhs)))(InputPosition(0, 0, 0)))))
              (rel, newLeftPattern, right, Set(), true)
            }
            else if (right.variable.get.name == name) {
              val newRightPattern = getNewNodePattern(right, Map.empty, Map(name -> Option(MapExpression(List((pkn, rhs)))(InputPosition(0, 0, 0)))))
              (rel, left, newRightPattern, Set(), true)
            }
            else (rel, left, right, Set(), false)
          }
          case _ => (rel, left, right, Set(), false)
        }
      }
      case andExpr@Ands(expressions) => {
        val (nodeLabels, nodeProperties, otherExpressions) = extractFromFilterExpression(andExpr)

        val leftPattern = getNewNodePattern(left, nodeLabels, nodeProperties)
        val rightPattern = getNewNodePattern(right, nodeLabels, nodeProperties)
        val _rel = rel

        if (otherExpressions.isEmpty) (_rel, leftPattern, rightPattern, Set(), true)
        else {
          if (otherExpressions.size > 1) {
            (_rel, leftPattern, rightPattern, Set(Ands(otherExpressions.toSet)(InputPosition(0, 0, 0))), true)
          }
          else {
            (_rel, leftPattern, rightPattern, Set(otherExpressions.head), true)
          }
        }
      }
      case _ => (rel, left, right, Set(), false)
    }
  }

  def expandPathPushDown(expression: Expression, right: NodePattern,
                         pep: Expand, ppc: PhysicalPlannerContext): (PhysicalPlan, Set[Expression]) = {
    val (nodeLabels, nodeProperties, otherExpressions) = extractFromFilterExpression(expression)

    val topExpandPath = bottomUpExpandPath(nodeLabels, nodeProperties, pep, ppc)

    if (otherExpressions.isEmpty) (topExpandPath, Set())
    else {
      if (otherExpressions.size > 1) (topExpandPath, Set(Ands(otherExpressions.toSet)(InputPosition(0, 0, 0))))
      else (topExpandPath, Set(otherExpressions.head))
    }
  }

  def bottomUpExpandPath(nodeLabels: Map[String, Seq[LabelName]], nodeProperties: Map[String, Option[Expression]],
                         pptNode: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = {
    pptNode match {
      case e@Expand(rel, right) =>
        val newPEP = bottomUpExpandPath(nodeLabels: Map[String, Seq[LabelName]], nodeProperties: Map[String, Option[Expression]], e.children.head, ppc)
        val expandRightPattern = getNewNodePattern(right, nodeLabels, nodeProperties)
        plans.Expand(rel, expandRightPattern)(newPEP, ppc)

      case r@RelationshipScan(rel, left, right) => {
        val leftPattern = getNewNodePattern(left, nodeLabels, nodeProperties)
        val rightPattern = getNewNodePattern(right, nodeLabels, nodeProperties)
        plans.RelationshipScan(rel, leftPattern, rightPattern)(ppc)
      }

      case _ => pptNode
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
    val props = (propCheck,node.properties) match {
      case (Some(Some(m:MapExpression)), Some(n:MapExpression)) => Some(MapExpression(m.items++n.items)(m.position))
      case (Some(Some(m:Expression)), Some(n:Expression)) => Some(Ands(Set(m,n))(m.position))
      case (Some(Some(m:MapExpression)), None) => Some(m)
      case (None, n) => n
      case (_,_) => None
    }
//    {
//      if (propCheck.isDefined) {
//        if (node.properties.isDefined)
//          Option(Ands(Set(node.properties.get, propCheck.get.get))(InputPosition(0, 0, 0)))
//        else propCheck.get
//      }
//      else node.properties
//    }
    NodePattern(node.variable, label, props, node.baseNode)(node.position)
  }

  def handleNodeAndsExpression(andExpr: Expression, pattern: NodePattern): (NodePattern, Set[Expression], Boolean) = {
    val (pushLabels, propertiesMap, notPushDown) = extractFromFilterExpression(andExpr)

    val newNodePattern = getNewNodePattern(pattern, pushLabels, propertiesMap)

    if (notPushDown.size > 1) (newNodePattern, Set(Ands(notPushDown.toSet)(InputPosition(0, 0, 0))), true)
    else (newNodePattern, notPushDown.toSet, true)
  }

  private def updatePropPushDownMap(filters: Expression, elementName: String, propKeyName: PropertyKeyName, operatorLiteral: String, rhs: Expression,
                                        propMap: mutable.Map[String, ArrayBuffer[(PropertyKeyName, Expression)]],
                                        propOpsMap: mutable.Map[String, ArrayBuffer[(PropertyKeyName, Expression)]],
                                        notPushDown: ArrayBuffer[Expression]
                                       ): Unit = {
    if (propMap.contains(elementName)) {
      //propMap(name).append((pkn, rhs))
      notPushDown += filters
    }
    else {
      propMap += elementName -> ArrayBuffer((propKeyName, rhs))
    }
    if (propOpsMap.contains(elementName)) {
      //propOpsMap(name).append((pkn, StringLiteral("EQUAL")(InputPosition(0, 0, 0))))
      notPushDown += filters
    }
    else {
      propOpsMap += elementName -> ArrayBuffer((propKeyName, StringLiteral(operatorLiteral)(InputPosition(0, 0, 0))))
    }
  }
}
