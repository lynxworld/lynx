package org.grapheco.lynx

import org.opencypher.v9_0.ast.{AliasedReturnItem, ReturnItemsDef}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait PhysicalPlanOptimizer {
  def optimize(plan: PPTNode): PPTNode
}

trait PPTNodeContext {
  def replaceWith(orginal: PPTNode, newNode: PPTNode): Unit

  val downStream: Option[PPTNode]
}

class AbstractPhysicalPlanOptimizer extends PhysicalPlanOptimizer {
  private val rules = ArrayBuffer[(PPTNode, PPTNodeContext) => Option[PPTNode]]()

  def optimize(plan: PPTNode): PPTNode = {
    //builds nodes map
    val parents = mutable.Map[PPTNode, Option[PPTNode]]()
    val transformed = mutable.Map[PPTNode, PPTNode]()
    buildTopDown(plan, None, parents, transformed)

    //do optimize
    rules.foreach(rule =>
      transformed.values.foreach(node => {
        val ret = rule(node, new PPTNodeContext() {
          //TODO: replace a->b, b->c
          override val downStream: Option[PPTNode] = parents(node)

          override def replaceWith(orginal: PPTNode, newNode: PPTNode): Unit = {
            transformed += orginal -> newNode
          }
        })

        ret match {
          case None =>
            transformed -= node

          case Some(node2) if node2 != node =>
            transformed += node -> node2

          case _ =>
        }
      }
      )
    )

    transformed(plan)
  }

  private def buildTopDown(node: PPTNode, parent: Option[PPTNode], parents: mutable.Map[PPTNode, Option[PPTNode]], transformed: mutable.Map[PPTNode, PPTNode]): Unit = {
    parents += node -> parent
    transformed += node -> node
    node.children.foreach(x => buildTopDown(x, Some(node), parents, transformed))
  }

  def add(rule: (PPTNode, PPTNodeContext) => Option[PPTNode]) = rules.insert(0, rule)
}

class PhysicalPlanOptimizerImpl extends AbstractPhysicalPlanOptimizer {
  add(removeNullProject)

  def removeNullProject(node: PPTNode, ctx: PPTNodeContext) = node match {
    case thisNode@PPTProject(ri: ReturnItemsDef) =>
      if (ri.items.forall {
        case AliasedReturnItem(expression, variable) => expression == variable
        case _ => false
      }) {
        ctx.downStream.map(pnode => {
          val newNode = pnode.withChildren(pnode.children.filterNot(_ == thisNode) ++ thisNode.children)
          ctx.replaceWith(pnode, newNode)
          newNode
        }
        )
      }
      else
        Some(thisNode)

    case _ =>
      Some(node)
  }
}