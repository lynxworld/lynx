package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.TreeNode
import org.grapheco.lynx.physical.plans.PhysicalPlan.empty
import org.grapheco.lynx.types.LynxType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait PhysicalPlan extends TreeNode{

  override type SerialType = PhysicalPlan

  override def children: Seq[PhysicalPlan] = Seq(left, right).flatten

  var left: Option[PhysicalPlan]

  var right: Option[PhysicalPlan]

  var profileMode: Boolean = false

  var cache_hits: Option[Long] = None

  var cache_misses: Option[Long] = None

  var estimated_rows: Option[Long] = None

  var db_hit: Option[Long] = None

  def schema: Seq[(String, LynxType)]

  def execute(implicit ctx: ExecutionContext): DataFrame

  def leaves: Seq[PhysicalPlan] = {
    val stack: mutable.ArrayStack[PhysicalPlan] = new mutable.ArrayStack[PhysicalPlan]()
    val leafs: mutable.ArrayBuffer[PhysicalPlan] = new ArrayBuffer[PhysicalPlan]()
    stack.push(this)
    while (stack.nonEmpty) {
      val p = stack.pop()
      if (p.children.isEmpty || p.children.forall(empty.eq)) leafs.append(p)
      else p.children.filterNot(empty.eq).foreach(stack.push)
    }
    leafs
  }

  def withChildren(children0: Seq[PhysicalPlan]): PhysicalPlan =
    withChildren(children0.headOption, children0.lift(1))

  def withChildren(left: Option[PhysicalPlan], right: Option[PhysicalPlan] = None): PhysicalPlan = {
    this.left = left
    this.right = right
    this
  }

  def ~> (andThen: PhysicalPlan): PhysicalPlan = this.~>(Option(andThen))

  def ~> (andThen: Option[PhysicalPlan]): PhysicalPlan = andThen.map(_.withChildren(Some(this))).getOrElse(this)

  def <~ (left: Option[PhysicalPlan], right: Option[PhysicalPlan] = None): PhysicalPlan = this.withChildren(left, right)

  def <~ (left: PhysicalPlan): PhysicalPlan = this.withChildren(Some(left), right)

  def <~ (left: PhysicalPlan, right: PhysicalPlan): PhysicalPlan = this.withChildren(Some(left), Some(right))

  override def description: String = (if (profileMode) s"<${db_hit.getOrElse(0)} rows>" else "") +
    s"[${this.schema.map(_._1).mkString(",")}]_$toString"

}

object PhysicalPlan {
  val empty: PhysicalPlan = new PhysicalPlan {
    override def schema: Seq[(String, LynxType)] = Seq.empty

    override var left: Option[PhysicalPlan] = None
    override var right: Option[PhysicalPlan] = None

    override def execute(implicit ctx: ExecutionContext): DataFrame = DataFrame.empty
  }
}

