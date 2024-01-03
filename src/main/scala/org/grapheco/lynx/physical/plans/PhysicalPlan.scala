package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.{LynxType, TreeNode}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait PhysicalPlan extends TreeNode{

  override type SerialType = PhysicalPlan

  override def children: Seq[PhysicalPlan] = Seq(left, right).flatten

  var left: Option[PhysicalPlan]

  var right: Option[PhysicalPlan]

  def schema: Seq[(String, LynxType)]

  def execute(implicit ctx: ExecutionContext): DataFrame

  def leaves: Seq[PhysicalPlan] = {
    val stack: mutable.ArrayStack[PhysicalPlan] = new mutable.ArrayStack[PhysicalPlan]()
    val leafs: mutable.ArrayBuffer[PhysicalPlan] = new ArrayBuffer[PhysicalPlan]()
    stack.push(this)
    while (stack.nonEmpty) {
      val p = stack.pop()
      if (p.children.isEmpty) leafs.append(p)
      else p.children.foreach(stack.push)
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

  def description: String = ""

//  override def toString: String = s"${this.getClass.getSimpleName}($description)[${this.schema.map(_._1).mkString(",")}]"

}

