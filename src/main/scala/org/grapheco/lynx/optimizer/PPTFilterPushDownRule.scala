package org.grapheco.lynx.optimizer


import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans
import org.grapheco.lynx.physical.plans.{Cross, Expand, Filter, Join, NodeScan, PhysicalPlan, RelationshipScan, Reverse, ShortestPath}
import org.grapheco.lynx.procedure.ProcedureExpression
import org.opencypher.v9_0.expressions.{Ands, Equals, Expression, FunctionInvocation, FunctionName, HasLabels, In, LabelName, ListLiteral, Namespace, NodePattern, Not, Ors, Property, RelationshipPattern, Variable}
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

  /**
   * @param pf    the PPTFilter
   * @param pnode the parent of PPTFilter, to rewrite PPTFilter
   * @param ppc   context
   * @return a seq and a flag, flag == true means push-down works
   */
  private def pptFilterPushDownRule(pf: Filter, ppc: PhysicalPlannerContext): (Seq[PhysicalPlan], Boolean) = {
    val (notPushDown, pushDown, labelMap) = getPushDownExpression(pf.expr, ppc)
    val pushDowns: Seq[Seq[(String, Expression)]] = foldPushDown(pushDown)
    pf.children match {
      case Seq(pns@NodeScan(pattern, optional)) =>
        val nodeScans = getNewPattern(pattern, pushDowns, labelMap).map(NodeScan(_, optional)(ppc))
        refactorPhysicalPlan(pushDowns, notPushDown, nodeScans, ppc)
      case Seq(prs@RelationshipScan(rel, left, right, optional)) =>
        val leftPatterns = getNewPattern(left, pushDowns, labelMap)
        val rightPatterns = getNewPattern(right, pushDowns, labelMap)
        val relationshipScans = leftPatterns.zip(rightPatterns).map(n => RelationshipScan(rel, n._1, n._2, optional)(ppc))
          .map(relationshipScan => reverseRelationScan(relationshipScan, ppc))
        refactorPhysicalPlan(pushDowns, notPushDown, relationshipScans, ppc)
      case Seq(path: ShortestPath) =>
        val ShortestPath(rel: RelationshipPattern, left: NodePattern, right: NodePattern, single: Boolean, resName: String) = path
        val leftPatterns = getNewPattern(left, pushDowns, labelMap)
        val rightPatterns = getNewPattern(right, pushDowns, labelMap)
        val shortestPaths = leftPatterns.zip(rightPatterns).map(n => ShortestPath(rel,n._1,n._2, single, resName)(ppc))
        refactorPhysicalPlan(pushDowns, notPushDown, shortestPaths, ppc)
      case Seq(pep@Expand(rel, right, optional)) =>
        // TODO The same type of relationship does not equal
        val newNotPushDown = notPushDown.filter(expr => {
          expr match {
            case Not(Equals(Variable(r), Variable(t))) => false
            case _ => true
          }
        })
        val expands = expandPathPushDown(labelMap, pushDowns, pep, ppc)
        refactorPhysicalPlan(pushDowns, newNotPushDown, expands, ppc)

      case Seq(pj@Join(filterExpr, isSingleMatch, bigTableIndex)) => if(pushDowns.length == 0) (null, false) else pptFilterThenJoin(pushDowns,notPushDown,labelMap, pj, ppc)
      case Seq(pc@Cross()) => { if(pushDowns.length == 0) (null, false) else
        pptFilterThenJoin(pushDowns,notPushDown,labelMap, pc, ppc)
      }
      case _ => (null, false)
    }
  }

  def getPushDownExpression(filterExpr:Expression, ppc: PhysicalPlannerContext)  = {
    val notPushDown: ArrayBuffer[Expression] = new ArrayBuffer[Expression]
    val pushDown: ArrayBuffer[Seq[(String, Expression)]] = new ArrayBuffer[Seq[(String, Expression)]]
    val labelMap = mutable.Map[String, Seq[LabelName]]()
    extractIndexPropFromFilterExpression(filterExpr, notPushDown, pushDown, labelMap, ppc)
    val (newNotPushDown, newPushDown) = getOneOfOrs(notPushDown, pushDown)
    (newNotPushDown, newPushDown, labelMap)
  }

  private def getOneOfOrs(notPushDown: ArrayBuffer[Expression], pushDown: ArrayBuffer[Seq[(String, Expression)]]): (ArrayBuffer[Expression], ArrayBuffer[Seq[(String, Expression)]]) = {
    val (orsExps, expr) = pushDown.partition(exprs => exprs.length>1)
    if(orsExps.length > 1) expr.append(orsExps.remove(0))
    val newNotPushDown: ArrayBuffer[Expression] = notPushDown ++ orsExps.map(ors => Ors(ors.map(_._2).toSet)(InputPosition(0,0,0)))
    (newNotPushDown, expr)
  }

  private def reverseRelationScan(relationshipScan: RelationshipScan, ppc: PhysicalPlannerContext): PhysicalPlan = {
    val rel = relationshipScan.rel
    if (relationshipScan.leftNode.properties.isEmpty && relationshipScan.rightNode.properties.nonEmpty) {
      Reverse()(RelationshipScan(reverseRelationPattern(rel), relationshipScan.rightNode, relationshipScan.leftNode, relationshipScan.optional)(ppc), ppc)
    } else relationshipScan
  }

  def reverseRelationPattern(rel: RelationshipPattern): RelationshipPattern =
    RelationshipPattern(rel.variable,rel.types,rel.length,rel.properties,rel.direction.reversed)(rel.position)


  private def pptFilterThenJoinPushDown(pushDowns: Seq[Seq[(String, Expression)]],
                                        labelMap: mutable.Map[String, Seq[LabelName]],
                                        pj: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = {
    val res = pj.children.map{
      case pn@NodeScan(pattern, optional) =>
        val nodeScans = getNewPattern(pattern, pushDowns, labelMap).map(plans.NodeScan(_, optional)(ppc))
        refactorJoinChildren(nodeScans)(ppc)
      case path : ShortestPath =>
        val ShortestPath(rel: RelationshipPattern, leftNode: NodePattern, rightNode: NodePattern, single: Boolean, resName: String) = path
        val leftPatterns = getNewPattern(leftNode, pushDowns, labelMap)
        val rightPatterns = getNewPattern(rightNode, pushDowns, labelMap)
        val shortestPaths = leftPatterns.zip(rightPatterns).map(n => ShortestPath(rel, n._1, n._2, single, resName)(ppc))
        refactorJoinChildren(shortestPaths)(ppc)
      case e: Expand =>
        val expands = expandPathPushDown(labelMap, pushDowns, e, ppc)
        refactorJoinChildren(expands)(ppc)
      case pr@RelationshipScan(rel, leftNode, rightNode, optional) =>
        val leftPatterns = getNewPattern(leftNode, pushDowns, labelMap)
        val rightPatterns = getNewPattern(rightNode, pushDowns, labelMap)
        val relationshipScans = leftPatterns.zip(rightPatterns).map(n => RelationshipScan(rel, n._1, n._2, optional)(ppc))
        refactorJoinChildren(relationshipScans)(ppc)
      case pjj@Join(filterExpr, isSingleMatch, joinType) => pptFilterThenJoinPushDown(pushDowns, labelMap, pjj, ppc)
      case pcc@Cross() => pptFilterThenJoinPushDown(pushDowns, labelMap, pcc, ppc)
      case f => f

    }
    pj.withChildren(res)
  }

  private def pptFilterThenJoin(pushDowns: Seq[Seq[(String, Expression)]],
                                notPushDown: ArrayBuffer[Expression],
                                labelMap: mutable.Map[String, Seq[LabelName]],
                                pj: PhysicalPlan, ppc: PhysicalPlannerContext): (Seq[PhysicalPlan], Boolean) = {
    val res = pptFilterThenJoinPushDown(pushDowns, labelMap, pj, ppc)

    notPushDown.size match {
      case 0 => (Seq(res), true)
      case 1 => (Seq(plans.Filter(notPushDown.head)(res, ppc)), true)
      case _ => {
        val expr = Ands(Set(notPushDown: _*))(InputPosition(0, 0, 0))
        (Seq(plans.Filter(expr)(res, ppc)), true)
      }
    }
  }

  private def refactorJoinChildren(physicalPlans: Seq[PhysicalPlan])(ppc: PhysicalPlannerContext): PhysicalPlan = {
    physicalPlans.length match {
      case 1 => physicalPlans.head
      case _ => plans.Ors()(physicalPlans, ppc)
    }
  }

  private def pptJoinPushDown(pj: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = {
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

  private def expandPathPushDown(labelMap: mutable.Map[String, Seq[LabelName]], pushDowns: Seq[Seq[(String, Expression)]]
                                 , pep: Expand, ppc: PhysicalPlannerContext ): Seq[PhysicalPlan] = {
    if(pushDowns.size == 0) {
      val (topExpandPath, relStartNodeHasProp) = bottomUpExpandPath(labelMap, Seq.empty, pep, ppc)
      var reactorExpandPath = topExpandPath
      if(!relStartNodeHasProp) reactorExpandPath = refactorTopExpandPath(topExpandPath, ppc)
      return Seq(reactorExpandPath)
    }
    pushDowns.map(pushDown => {
      val (topExpandPath, relStartNodeHasProp) = bottomUpExpandPath(labelMap, pushDown, pep, ppc)
      var reactorExpandPath = topExpandPath
      if(!relStartNodeHasProp) reactorExpandPath = refactorTopExpandPath(topExpandPath, ppc)
      reactorExpandPath
    })
  }

  private def refactorTopExpandPath(topExpandPath: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = {
    val leftExpandSeq = new ArrayBuffer[(RelationshipPattern,NodePattern)]()
    val rightExpandSeq = new ArrayBuffer[(RelationshipPattern,NodePattern)]()
    var relationshipScan:RelationshipScan = null
    var isOptional = false

    def getExpandLeft(expandPath: PhysicalPlan): Boolean = {
      expandPath match {
        case  e@Expand(rel, right, optional) => leftExpandSeq.append((rel, right))
          isOptional = optional
          if(right.properties.nonEmpty) {
            getExpandRight(e)
            false
          } else getExpandLeft(e.children.head)
        case r@RelationshipScan(rel, left, right, optional) => true
        case _ => true
      }
    }
    def getExpandRight(expandPath: PhysicalPlan): Unit = {
      expandPath match {
        case e@Expand(rel, right, optional) =>
          isOptional = optional
          val newRel = RelationshipPattern(rel.variable,rel.types,rel.length,rel.properties,rel.direction.reversed)(rel.position)
          rightExpandSeq.append((newRel, right))
          getExpandRight(e.children.head)
        case r@RelationshipScan(rel, left, right, optional) =>
          val newRel = RelationshipPattern(rel.variable,rel.types,rel.length,rel.properties,rel.direction.reversed, optional)(rel.position)
          relationshipScan = plans.RelationshipScan(newRel,left, right)(ppc)
      }
    }
    val isRefactor = getExpandLeft(topExpandPath)
    if(isRefactor) topExpandPath
    else {
      var index = 0
      val rightRefactorIter: Iterator[(NodePattern, RelationshipPattern, NodePattern)] = rightExpandSeq.map(e => {
        index += 1
        if (index == rightExpandSeq.length) {
          (e._2, e._1, relationshipScan.rightNode)
        } else {
          (e._2, e._1, rightExpandSeq(index)._2)
        }
      }).toIterator
      val item = rightRefactorIter.next()
      var rightPhysicalPlan: PhysicalPlan = plans.RelationshipScan(item._2, item._1, item._3)(ppc)
      rightRefactorIter.foreach(item => {
        rightPhysicalPlan = plans.Expand(item._2, item._3, isOptional)(rightPhysicalPlan, ppc)
      })
      rightPhysicalPlan = plans.Expand(relationshipScan.rel, relationshipScan.leftNode, isOptional)(rightPhysicalPlan, ppc)


      var leftPhysicalPlan: PhysicalPlan = null
      if(leftExpandSeq.length>1){
        val leftExpandSeqIter = leftExpandSeq.reverse.toIterator
        val leftExpandSeqIter_1 = leftExpandSeqIter.next()
        val leftExpandSeqIter_2 = leftExpandSeqIter.next()
        leftPhysicalPlan = plans.RelationshipScan(leftExpandSeqIter_2._1, leftExpandSeqIter_1._2, leftExpandSeqIter_2._2)(ppc)
        leftExpandSeqIter.foreach(l => {
          leftPhysicalPlan = plans.Expand(l._1, l._2, isOptional)(leftPhysicalPlan, ppc)
        })
        plans.Link()(plans.Reverse()(rightPhysicalPlan, ppc), leftPhysicalPlan, ppc)
      }else {
        plans.Reverse()(rightPhysicalPlan, ppc)
      }
    }

  }

  private def bottomUpExpandPath(nodeLabels: mutable.Map[String, Seq[LabelName]], pushDown: Seq[(String, Expression)]
                                 , pptNode: PhysicalPlan, ppc: PhysicalPlannerContext): (PhysicalPlan, Boolean) = {
    var relStartNodeHasProp = false
    val physicalPlan:PhysicalPlan = pptNode match {
      case e@Expand(rel, right, optional) =>
        val newPEP = bottomUpExpandPath(nodeLabels, pushDown, e.children.head, ppc)
        relStartNodeHasProp = newPEP._2
        val expandRightPattern = getNewPattern(right, Seq(pushDown) ,nodeLabels).head
        plans.Expand(rel, expandRightPattern, optional)(newPEP._1, ppc)

      case r@RelationshipScan(rel, left, right, optional) => {
        val leftPattern = getNewPattern(left, Seq(pushDown) ,nodeLabels).head
        val rightPattern = getNewPattern(right, Seq(pushDown) ,nodeLabels).head
        val relationshipScan = plans.RelationshipScan(rel, leftPattern, rightPattern, optional)(ppc)
        if(leftPattern.properties.nonEmpty) {
          relStartNodeHasProp = true
          relationshipScan
        }else if(rightPattern.properties.nonEmpty){
          relStartNodeHasProp = true
          plans.Reverse()(plans.RelationshipScan(reverseRelationPattern(rel), rightPattern, leftPattern, optional)(ppc),ppc)
        }else{
          relationshipScan
        }
      }
      case _ => pptNode
    }
    (physicalPlan, relStartNodeHasProp)
  }

  private def refactorPhysicalPlan(pushDowns: Seq[Seq[(String, Expression)]]
                                   ,notPushDown:ArrayBuffer[Expression]
                                   ,physicalPlans: Seq[PhysicalPlan]
                                   ,ppc: PhysicalPlannerContext): (Seq[PhysicalPlan], Boolean) = {
    pushDowns.length match {
      case len if len < 2 =>
        notPushDown.length match {
          case 0 => (physicalPlans, true)
          case 1 => (Seq(Filter(notPushDown.head)(physicalPlans.head, ppc)),true)
          case _ => (Seq(Filter(Ands(notPushDown.toSet)(InputPosition(0,0,0)))(physicalPlans.head, ppc)),true)
        }
      case _ => notPushDown.length match {
        case 0 => (Seq(plans.Ors()(physicalPlans,ppc)), true)
        case 1 => (Seq(Filter(notPushDown.head)(plans.Ors()(physicalPlans,ppc), ppc)),true)
        case _ => (Seq(Filter(Ands(notPushDown.toSet)(InputPosition(0,0,0)))(plans.Ors()(physicalPlans,ppc), ppc)),true)
      }
    }
  }

  def foldPushDown(pushDown: ArrayBuffer[Seq[(String, Expression)]] ): Seq[Seq[(String, Expression)]] = {
    if(pushDown.isEmpty) Seq.empty else
      pushDown.foldLeft(Seq(Seq.empty[(String, Expression)])) {
        (acc, next) => for {
          a <- acc
          b <- next
        } yield a :+ b
      }
  }

  def getNewPattern(pattern: NodePattern, pushDowns: Seq[Seq[(String, Expression)]], labelMap: mutable.Map[String, Seq[LabelName]]): Seq[NodePattern] = {
    val labelCheck = labelMap.get(pattern.variable.get.name)
    val label = {
      if (labelCheck.isDefined) (labelCheck.get ++ pattern.labels).distinct
      else pattern.labels
    }
    val props: Seq[Option[ListLiteral]] = pattern.properties match {
      case l@Some(ListLiteral(expressions)) => pushDowns.map(seq => Some(ListLiteral(expressions ++ seq.filter(_._1 == pattern.variable.get.name).map(_._2))(l.get.position)))
      case _ => pushDowns.map(seq => Some(ListLiteral(seq.filter(_._1 == pattern.variable.get.name).map(_._2))(pattern.position)))
    }
    if(props.isEmpty || pushDowns.head.isEmpty){
      Seq(NodePattern(pattern.variable, label, pattern.properties, pattern.baseNode)(pattern.position))
    }else
      props.map(prop => {
        val props = if(prop.get.expressions.isEmpty) None else prop
        NodePattern(pattern.variable, label, props, pattern.baseNode)(pattern.position)
      })
  }

  def extractIndexPropFromFilterExpression(filters: Expression
                                           , notPushDown: ArrayBuffer[Expression]
                                           , pushDown: ArrayBuffer[Seq[(String, Expression)]]
                                           , labelMap: mutable.Map[String, Seq[LabelName]]
                                           , ppc: PhysicalPlannerContext): Unit = {
    filters match {
      case e@Equals(lhs, rhs) => lhs match {
        case Property(expr, pkn) => rhs match {
          // Do not push down the Equals is rhs is a Variable.
          //          case Variable(v) => notPushDown += e
          case _ => expr match {
            case Variable(name) => pushDown.append(Seq((name, e)))
            case _ => notPushDown += e
          }
        }
        case ProcedureExpression(FunctionInvocation(Namespace(List()), FunctionName("id"), false, Vector(Variable(nodeName)))) =>
          pushDown.append(Seq((nodeName, e)))
        case Variable(n) => pushDown.append(Seq((n, e)))
        case _ => notPushDown += e
      }
      case hl@HasLabels(expr, labels) => {
        expr match {
          case Variable(name) => {
            labelMap += name -> labels
          }
          case _ => notPushDown += filters // TODO: expand others expression
        }
      }
      case in@In(lhs, rhs) =>
        lhs match {
          case Property(expr, pkn) => rhs match {
            //            case Variable(v) =>  notPushDown += in
            case _ => expr match {
              case Variable(name) => pushDown.append(Seq((name, in)))
              case _ => notPushDown += in
            }
          }
          case ProcedureExpression(FunctionInvocation(Namespace(List()), FunctionName("id"), false, Vector(Variable(nodeName)))) =>
            pushDown.append(Seq((nodeName, in)))
          case  Variable(l) =>
            rhs match {
              case ListLiteral(expressions) =>
                pushDown.append(Seq((l, in)))
              case _ => notPushDown += in
            }
          case _ => notPushDown += in
        }
      case o@Ors(orExpress) =>
        val temp_notPushDown = new ArrayBuffer[Expression]()
        val temp_pushDown = new ArrayBuffer[Seq[(String, Expression)]]()
        orExpress.foreach(exp => extractIndexPropFromFilterExpression(exp, temp_notPushDown, temp_pushDown, labelMap, ppc))
        if(temp_pushDown.length == orExpress.size) pushDown.append(temp_pushDown.flatMap(s=>s))
        else notPushDown += o
      case a@Ands(andExpress) => andExpress.foreach(exp => extractIndexPropFromFilterExpression(exp, notPushDown, pushDown, labelMap, ppc))
      case other => notPushDown += other
    }
  }
}
