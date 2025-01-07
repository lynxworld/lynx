package org.grapheco.lynx.optimizer
import org.grapheco.lynx.physical.plans.{Apply, Filter, Project, PhysicalPlan}
import org.opencypher.v9_0.expressions.Variable

object ApplyPushDownRule extends PhysicalPlanOptimizerRule {
  override def ops: Seq[PartialFunction[PhysicalPlan, PhysicalPlan]] = Seq(APPLY_PUSH_DOWN)
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
    case apply: Apply =>
      val A = apply.left.get
      val B = apply.right.get
      
      // 递归检查右子树是否只包含 FromArgument
      def containsOnlyFromArgument(plan: PhysicalPlan): Boolean = plan match {
        case _: FromArgument => true
        case p if p.children.nonEmpty => 
          p.children.length == 1 && containsOnlyFromArgument(p.children.head)
        case _ => false
      }
      
      // 如果右子树只包含 FromArgument，则重组查询计划
      if (containsOnlyFromArgument(B)) {
        // 获取右子树中除 FromArgument 外的所有操作
        def extractOperations(plan: PhysicalPlan): Option[PhysicalPlan] = plan match {
          case _: FromArgument => None
          case p if p.children.nonEmpty => 
            val childOp = extractOperations(p.children.head)
            childOp match {
              case Some(child) => 
                p.withChildren(Some(child), None)
                Some(p)
              case None => Some(p)
            }
          case _ => Some(plan)
        }
        
        // 重组查询计划
        val operations = extractOperations(B)
        operations match {
          case Some(ops) =>
            ops.withChildren(Some(A), None)
            ops
          case None => A
        }
      } else {
        apply
      }
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
      val B = apply.right
      val returnItemNames = A.get.schema.map(_._1)

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
}
