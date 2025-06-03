package org.grapheco.lynx.optimizer
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.physical.plans.{Apply, Cross, Expand, Filter, Join, NodeScan, PhysicalPlan, Project, RelationshipScan, ShortestPath}
import org.grapheco.lynx.procedure.ProcedureExpression
import org.opencypher.v9_0.expressions.{Ands, Equals, Expression, FunctionInvocation, FunctionName, ListLiteral, LogicalVariable, MapExpression, Namespace, NodePattern, PropertyKeyName, SignedDecimalIntegerLiteral, StringLiteral, Variable}
import org.opencypher.v9_0.util.InputPosition

object ApplyPushDownRule extends PhysicalPlanOptimizerRule {
  //  override def ops: Seq[PartialFunction[PhysicalPlan, PhysicalPlan]] = Seq(APPLY_PUSH_DOWN)

  override def apply(plan: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = optimizeBottomUp(plan,{
    case apply: Apply =>
      val left = apply.left
      val right: Option[PhysicalPlan] = apply.right
      if (right.nonEmpty) {
        val newRight = putVariableToRight(left.get, right.get, ppc)
        Apply()(left.get, newRight, ppc)
      } else apply
    case cross: Cross =>
      val left = cross.left
      val right: Option[PhysicalPlan] = cross.right
      if(right.nonEmpty) {
        val newRight = putVariableToRight(left.get, right.get, ppc)
        Cross()(left.get, newRight, ppc)
      }else cross
    case join@Join(expr, isSingleMatch, joinType) =>
      val left = join.left
      val right: Option[PhysicalPlan] = join.right
      if(right.nonEmpty) {
        val newRight = putVariableToRight(left.get, right.get, ppc)
        Join(expr, isSingleMatch, joinType)(left.get, newRight, ppc)
      }else join
  })

  private def putVariableToRight(left: PhysicalPlan, right: PhysicalPlan, ppc: PhysicalPlannerContext): PhysicalPlan = {
    val returnItemNames: Seq[String] = left.schema.map(_._1)
    implicit val ec = ppc
    optimizeBottomUp(right, {
      case pns@NodeScan(nodePattern, optional) =>
        if(returnItemNames.contains(nodePattern.variable.get.name)) NodeScan(addIdToNodePattern(nodePattern), optional)(ppc)
        else pns
      case pns@RelationshipScan(rel, leftNode, rightNode, optional) =>
        var left = leftNode
        var right = rightNode
        if(returnItemNames.contains(leftNode.variable.get.name)) left = addIdToNodePattern(leftNode)
        if(returnItemNames.contains(rightNode.variable.get.name)) right = addIdToNodePattern(rightNode)
        RelationshipScan(rel, left, right, optional)(ppc)
      case pns@Expand(rel, nodePattern, optional) =>
        if(returnItemNames.contains(nodePattern.variable.get.name)) Expand(rel, addIdToNodePattern(nodePattern), optional)(pns.children.head, ppc)
        else pns
      case pns@ShortestPath(rel, startNode, endNode, single, resName) =>
        var start = startNode
        var end = endNode
        if(returnItemNames.contains(startNode.variable.get.name)) start = addIdToNodePattern(startNode)
        if(returnItemNames.contains(endNode.variable.get.name)) end = addIdToNodePattern(endNode)
        ShortestPath(rel, start, end, single, resName)(ppc)
    })
  }

  private val REMOVE_USELESS_APPLY: PartialFunction[PhysicalPlan, PhysicalPlan] = {
    /* rule 1: Remove useless Apply, eg:
    Apply
     ╟──[A]
     ╙──[B]
         ║
        [X]*
         ╙──FromArgument
     ======== Replace to: =======
     [B]
      ║
     [X]*
      ║
     [A]
     */
    case apply: Apply => apply
  }

  private val APPLY_PUSH_DOWN: PartialFunction[PhysicalPlan, PhysicalPlan] = {
    /* rule 2: Push down Apply, eg
    Apply
      ╟──[A]
      ╙──[B]*(use result of apply)
          ║
         [X]
    ======== Replace to: =======
    [B]*(use result of apply)
     ╙──Apply
          ╟──[A]
          ╙──[X]
    */
    case apply:Apply =>
      val A = apply.left
      val B: Option[PhysicalPlan] = apply.right
      val returnItemNames: Seq[String] = A.get.schema.map(_._1)

      while (apply.right.isDefined
        //        && apply.right.get.children.length==1
        && extraUsage(apply.right.get).exists(returnItemNames.contains)) {
        apply.pushRightDownLeft
        //        val r = apply.right.get
        //        apply.right = r.left
        //        r.left = Some(apply)
      }
      if (apply.right == B) apply else B.get
  }

  private def extraUsage(p: PhysicalPlan): Seq[String] = p match {
    case _@Filter(expr) => expr.findByAllClass[Variable].map(_.name)
    case _ => Seq.empty
  }
  private def addBuiltInIdFilter(n: LogicalVariable, ppc: PhysicalPlannerContext): Expression = {
    val id_n = ProcedureExpression(FunctionInvocation(FunctionName("id")(InputPosition(0,0,0)),false,Vector(n))(InputPosition(0,0,0)))(ppc.runnerContext.procedureRegistry)
    Equals(id_n, id_n)(InputPosition(0,0,0))
  }
  def addIdToNodePattern(nodePattern: NodePattern)(implicit ppc: PhysicalPlannerContext): NodePattern = {
    if(nodePattern.variable.isEmpty) nodePattern
    else{
      val expression = addBuiltInIdFilter(nodePattern.variable.get, ppc)
      val newProps = nodePattern.properties match {
        case l@Some(ListLiteral(expressions)) => Some(ListLiteral(expressions ++ Seq(expression))(l.get.position))
        case None => Some(ListLiteral(Seq(expression))(InputPosition(0,0,0)))
        case _ => None
      }
      NodePattern(nodePattern.variable, nodePattern.labels, newProps, nodePattern.baseNode)(nodePattern.position)
    }
  }
}
