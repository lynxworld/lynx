package org.grapheco.lynx

import org.opencypher.v9_0.ast.AliasedReturnItem
import org.opencypher.v9_0.expressions.{Ands, Equals, Expression, FunctionInvocation, HasLabels, LabelName, Literal, LogicalVariable, MapExpression, NodePattern, Not, Ors, Property, PropertyKeyName, RelationshipPattern, Variable}
import org.opencypher.v9_0.util.InputPosition

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
    NodeFilterPushDownRule,
    JoinReferenceRule,
    JoinTableSizeEstimateRule
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
        case Seq(pj@PPTJoin(filterExpr, bigTableIndex)) => {
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
      case pjj@PPTJoin(filterExpr, bigTableIndex) => pptJoinChildrenMap(pjj, ppc)
      case f => f
    }
    pj.withChildren(res)
  }

  def pptFilterThenJoinChildrenMap(propArray:ArrayBuffer[(String, PropertyKeyName, Expression)], labelArray:ArrayBuffer[(String, Seq[LabelName])], pj: PPTJoin, ppc: PhysicalPlannerContext): PPTNode = {
   val res = pj.children.map {
      case pn@PPTNodeScan(pattern) => {
        val schema = pattern.variable.get.name
        val propsFilter = propArray.filter(f => f._1 == schema).map(f => (f._2, f._3))
        val labelFilter = labelArray.filter(f => f._1 == schema).map(f => f._2)

        val props = {
          if (propsFilter.nonEmpty) Option(MapExpression(propsFilter.toList)(pattern.position))
          else pattern.properties
        }
        val labels = {
          if (labelFilter.nonEmpty) labelFilter.head
          else pattern.labels
        }
        PPTNodeScan(NodePattern(pattern.variable, labels, props,pattern.baseNode)(pattern.position))(ppc)
      }
      case pjj@PPTJoin(filterExpr, bigTableIndex) => pptFilterThenJoinChildrenMap(propArray, labelArray, pjj, ppc)
      case f => f
    }
    pj.withChildren(res)
  }

  def pptFilterThenJoin(filters: Expression, pj:PPTJoin, ppc: PhysicalPlannerContext): (Seq[PPTNode], Boolean) ={
    val propertyArray:ArrayBuffer[(String, PropertyKeyName, Expression)] = ArrayBuffer()
    val labelArray:ArrayBuffer[(String, Seq[LabelName])] = ArrayBuffer()
    extractParamsFromFilterExpression(propertyArray, labelArray, filters)
    val res = pptFilterThenJoinChildrenMap(propertyArray,labelArray,pj, ppc)
    (Seq(res), true)
  }
  def extractParamsFromFilterExpression(propArray:ArrayBuffer[(String, PropertyKeyName, Expression)], labelArray:ArrayBuffer[(String, Seq[LabelName])], filters:Expression): Unit = {
    filters match {
      case e@Equals(Property(map, pkn), rhs) => propArray.append((map.asInstanceOf[Variable].name, pkn, rhs))
      case hl@HasLabels(expression, labels) => labelArray.append((expression.asInstanceOf[Variable].name, labels))
      case a@Ands(andExpress) => andExpress.foreach(exp => extractParamsFromFilterExpression(propArray, labelArray, exp))
    }
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
      case Seq(pj@PPTJoin(filterExpr, bigTableIndex)) => pptFilterThenJoin(exprs, pj, ppc)
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
        val leftNodeProperties: mutable.Set[(PropertyKeyName, Expression)] = mutable.Set.empty
        val rightNodeProperties: mutable.Set[(PropertyKeyName, Expression)] = mutable.Set.empty
        var leftPosition: InputPosition = null
        var rightPosition: InputPosition = null
        val otherExpressions = mutable.Set[Expression]()

        expressions.foreach {
          case hl@HasLabels(expr, labels) =>
            nodeLabels += expr.asInstanceOf[Variable].name -> labels

          case eq@Equals(Property(expr, propertyKey), value) =>
            val name = expr.asInstanceOf[Variable].name
            if (name == left.variable.get.name){
              leftNodeProperties.add((propertyKey, value))
              leftPosition = eq.position
            }else{
              rightNodeProperties.add((propertyKey, value))
              rightPosition = eq.position
            }

          case other => otherExpressions.add(other)
        }
        val leftProps = {
          if (leftPosition != null) Option(MapExpression(leftNodeProperties.toList)(leftPosition))
          else left.properties
        }
        val rightProps = {
          if (leftPosition != null) Option(MapExpression(rightNodeProperties.toList)(rightPosition))
          else right.properties
        }
        val leftPattern = getNewNodePattern(left, nodeLabels.toMap, leftProps)
        val rightPattern = getNewNodePattern(right, nodeLabels.toMap, rightProps)

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
    val nodeProperties = mutable.Map[String, mutable.Set[(PropertyKeyName, Expression)]]()
    val otherExpressions = mutable.Set[Expression]()
    var andsPosition: InputPosition = null
    val positionMap = mutable.Map[String, InputPosition]()

    expression match {
      case a@Ands(exprs) => {
        andsPosition = a.position
        exprs.foreach {
          case h@HasLabels(expression, labels) =>
            nodeLabels += expression.asInstanceOf[Variable].name -> labels

          case eq@Equals(Property(expression, propertyKey), value) =>
            val varName = expression.asInstanceOf[Variable].name
            if (nodeProperties.contains(varName)){
              nodeProperties(varName).add((propertyKey, value))
            }else{
              nodeProperties += varName -> mutable.Set[(PropertyKeyName, Expression)]((propertyKey, value))
              positionMap += varName -> eq.position
            }

          case other => otherExpressions.add(other)
        }
      }
      case other => otherExpressions.add(other)
    }

    val nodePropsMap = nodeProperties.toList.map(f => f._1 -> Option(MapExpression(f._2.toList)(positionMap(f._1)))).toMap

    val topExpandPath = bottomUpExpandPath(nodeLabels.toMap, nodePropsMap, pep, ppc)

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
        val expandRightPattern = getNewNodePattern(right, nodeLabels, nodeProperties.getOrElse(right.variable.get.name,right.properties))
        PPTExpandPath(rel, expandRightPattern)(newPEP, ppc)

      case r@PPTRelationshipScan(rel, left, right) => {
        val leftPattern = getNewNodePattern(left, nodeLabels, nodeProperties.getOrElse(left.variable.get.name, left.properties))
        val rightPattern = getNewNodePattern(right, nodeLabels, nodeProperties.getOrElse(right.variable.get.name,right.properties))
        PPTRelationshipScan(rel, leftPattern, rightPattern)(ppc)
      }
    }
  }

  def getNewNodePattern(node: NodePattern, nodeLabels: Map[String, Seq[LabelName]],
                        nodeProperties: Option[Expression]): NodePattern = {
    val label = nodeLabels.getOrElse(node.variable.get.name, node.labels)

    NodePattern(node.variable, label, nodeProperties, node.baseNode)(node.position)
  }

  def handleNodeAndsExpression(exprs: Set[Expression], pattern: NodePattern,
                               andsPosition: InputPosition): (NodePattern, Set[Expression], Boolean) = {
    val propertiesSet: mutable.Set[(PropertyKeyName, Expression)] = mutable.Set.empty
    var propertyPosition: InputPosition = null

    val otherExpressions: mutable.Set[Expression] = mutable.Set.empty
    var pushLabels: Seq[LabelName] = pattern.labels
    var newNodePattern: NodePattern = null
    exprs.foreach {
      case e@Equals(Property(map, pkn), rhs) => {
        propertiesSet.add((pkn, rhs))
        propertyPosition = e.position
      }

      case hl@HasLabels(expression, labels) => pushLabels = labels
      case other => otherExpressions.add(other)
    }

    val props = {
      if (propertyPosition != null) Option(MapExpression(propertiesSet.toList)(propertyPosition))
      else pattern.properties
    }
    newNodePattern = NodePattern(pattern.variable, pushLabels, props, pattern.baseNode)(pattern.position)

    if (otherExpressions.size > 1) (newNodePattern, Set(Ands(otherExpressions.toSet)(andsPosition)), true)
    else (newNodePattern, otherExpressions.toSet, true)
  }
}

object JoinReferenceRule extends PhysicalPlanOptimizerRule {

  def checkNodeReference(pattern: NodePattern): (Seq[((LogicalVariable, PropertyKeyName), Expression)], Option[NodePattern]) = {
    val variable = pattern.variable
    val properties = pattern.properties

    if (properties.isDefined){
      val MapExpression(items) = properties.get.asInstanceOf[MapExpression]
      val filter1 = items.filterNot(item => item._2.isInstanceOf[Literal])
      val filter2 = items.filter(item => item._2.isInstanceOf[Literal])
      val newPattern = {
        if (filter2.nonEmpty){
          NodePattern(variable, pattern.labels, Option(MapExpression(filter2)(properties.get.position)), pattern.baseNode)(pattern.position)
        }
        else NodePattern(variable, pattern.labels, None, pattern.baseNode)(pattern.position)
      }
      if (filter1.nonEmpty) (filter1.map(f => (variable.get, f._1) -> f._2), Some(newPattern))
      else (Seq.empty, None)
    }
    else (Seq.empty, None)
  }

  def checkExpandPath(pe: PPTExpandPath, ppc:PhysicalPlannerContext): (PPTNode, Seq[((LogicalVariable, PropertyKeyName), Expression)]) = {
    pe.children match {
      case Seq(pr@PPTRelationshipScan(rel, leftPattern, rightPattern)) =>{
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
      case Seq(pe2@PPTExpandPath(rel, rightPattern)) =>{
        val res = checkExpandPath(pe2, ppc)
        val rightChecked = checkNodeReference(rightPattern)

        val newPPTExpandPath = {
          if (rightChecked._2.nonEmpty){
            PPTExpandPath(rel, rightChecked._2.get)(res._1, ppc)
          }
          else pe2.withChildren(Seq(res._1))
        }

        (newPPTExpandPath, res._2 ++ rightChecked._1)
      }
    }
  }

  def joinReferenceRule(table: PPTNode, ppc:PhysicalPlannerContext): (PPTNode, Seq[((LogicalVariable, PropertyKeyName), Expression)]) = {
    var referenceProperty = Seq[((LogicalVariable, PropertyKeyName), Expression)]()
    val newTable = table match {
      case ps@PPTNodeScan(pattern)=>{
        val checked = checkNodeReference(pattern)
        referenceProperty = referenceProperty ++ checked._1
        if (checked._2.isDefined){
          PPTNodeScan(checked._2.get)(ppc)
        }
        else ps
      }
      case pr@PPTRelationshipScan(rel, leftPattern, rightPattern) =>{
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
      case pe@PPTExpandPath(rel, rightPattern) =>{
        val res = checkExpandPath(pe, ppc)
        referenceProperty ++= res._2
        res._1
      }
      case pm@PPTMerge(mergeSchema, mergeOps) =>{
        pm.children.head match {
          case pj2@PPTJoin(filterExpr, bigTableIndex) => {
            pm.withChildren(Seq(joinRecursion(pj2, ppc)))
          }
          case _ =>{
            pm
          }
        }
      }
      case pj1@PPTJoin(filterExpr, bigTableIndex) => {
        joinRecursion(pj1, ppc)
      }
    }
    (newTable, referenceProperty)
  }

  def joinRecursion(pj: PPTJoin, ppc: PhysicalPlannerContext): PPTNode = {
    var table1 = pj.children.head
    var table2 = pj.children.last
    var referenceProperty = Seq[((LogicalVariable, PropertyKeyName), Expression)]()

    val res1 = joinReferenceRule(table1, ppc)
    val res2 = joinReferenceRule(table2, ppc)
    table1 = res1._1
    table2 = res2._1
    referenceProperty ++= res1._2
    referenceProperty ++= res2._2

    if (referenceProperty.nonEmpty){
      referenceProperty.length match {
        case 1 => {
          val ksv = referenceProperty.head
          val filter = Equals(Property(ksv._1._1, ksv._1._2)(ksv._1._1.position), ksv._2)(ksv._1._1.position)
          PPTJoin(Option(filter))(table1, table2, ppc)
        }
        case _ =>{
          val setExprs = mutable.Set[Expression]()
          referenceProperty.foreach(f => {
            val filter = Equals(Property(f._1._1, f._1._2)(f._1._1.position), f._2)(f._1._1.position)
            setExprs.add(filter)
          })
          PPTJoin(Option(Ands(setExprs.toSet)(referenceProperty.head._1._1.position)))(table1, table2, ppc)
        }
      }
    }
    else pj
  }

  override def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode = optimizeBottomUp(plan,
    {
      case pnode: PPTNode =>
        pnode.children match {
          case Seq(pj@PPTJoin(filterExpr, bigTableIndex)) =>{
            val res1 = joinRecursion(pj, ppc)
            pnode.withChildren(Seq(res1))
          }
          case _ => pnode
        }
    }
  )
}

object JoinTableSizeEstimateRule extends PhysicalPlanOptimizerRule {

  override def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode = optimizeBottomUp(plan,
    {
      case pnode: PPTNode =>{
        pnode.children match {
          case Seq(pj@PPTJoin(filterExpr, bigTableIndex)) => {
            val res = joinRecursion(pj, ppc)
            pnode.withChildren(Seq(res))
          }
          case _ => pnode
        }
      }
    }
  )

  def estimateNodeRow(pattern: NodePattern, graphModel: GraphModel): Long = {
    val countArray = ArrayBuffer[Long]()
    val labels = pattern.labels.map(l => l.name)
    val prop = pattern.properties.map({
      case MapExpression(items)=>{
        items.map(p => {
          p._2 match {
            case Property(a, b) => p._1.name
            case _ => p._1.name
          }
        })
      }
    })

    labels.foreach(f => countArray += graphModel.estimateNodeLabel(f))
    if (prop.isDefined) prop.get.foreach(f => countArray += graphModel.estimateNodeProperty(f))
    countArray.min
  }

  def estimateRelationshipRow(rel: RelationshipPattern, left: NodePattern, right: NodePattern, graphModel: GraphModel): Long = {
    graphModel.estimateRelationship(rel.types.head.name)
  }

  def estimate(table: PPTNode, ppc: PhysicalPlannerContext): Long = {
    table match {
      case ps@PPTNodeScan(pattern) => estimateNodeRow(pattern, ppc.runnerContext.graphModel)
      case pr@PPTRelationshipScan(rel, left, right) => estimateRelationshipRow(rel, left, right, ppc.runnerContext.graphModel)
    }
  }

  def estimateTableSize(parent: PPTJoin, table1: PPTNode, table2: PPTNode, ppc: PhysicalPlannerContext): PPTNode = {
    val estimateTable1 = estimate(table1, ppc)
    val estimateTable2 = estimate(table2, ppc)
    if (estimateTable1 < estimateTable2) PPTJoin(parent.filterExpr, 1)(table1, table2, ppc)
    else PPTJoin(parent.filterExpr, 0)(table1, table2, ppc)
  }

  def joinRecursion(parent: PPTJoin, ppc: PhysicalPlannerContext): PPTNode ={
    val t1 = parent.children.head
    val t2 = parent.children.last

    val table1 = t1 match {
      case pj@PPTJoin(filterExpr, bigTableIndex) => joinRecursion(pj, ppc)
      case pm@PPTMerge(mergeSchema, mergeOps) => {
        val res = joinRecursion(pm.children.head.asInstanceOf[PPTJoin], ppc)
        pm.withChildren(Seq(res))
      }
      case _ => t1
    }
    val table2 = t2 match {
      case pj@PPTJoin(filterExpr, bigTableIndex) => joinRecursion(pj, ppc)
      case pm@PPTMerge(mergeSchema, mergeOps) => {
        val res = joinRecursion(pm.children.head.asInstanceOf[PPTJoin], ppc)
        pm.withChildren(Seq(res))
      }
      case _ => t2
    }

    if ((table1.isInstanceOf[PPTNodeScan] || table1.isInstanceOf[PPTRelationshipScan])
      && (table2.isInstanceOf[PPTNodeScan] || table2.isInstanceOf[PPTRelationshipScan])){
      estimateTableSize(parent, table1, table2, ppc)
    }
    else PPTJoin(parent.filterExpr, parent.bigTableIndex)(table1, table2, ppc)
  }
}
