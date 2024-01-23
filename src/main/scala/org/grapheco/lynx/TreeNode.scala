package org.grapheco.lynx

import scala.annotation.tailrec
import scala.collection.mutable

trait TreeNode{
  type SerialType <: TreeNode
  def children: Seq[SerialType] = Seq(left, right).flatten

  var left: Option[SerialType]
  var right: Option[SerialType]

  def pretty: String = recTreeToString(List(this), "", Nil).mkString("\n")

  def description: String = this.toString

  def recTreeToString(toPrint: List[TreeNode], prefix: String, stack: List[List[TreeNode]]): Seq[String] = {
    toPrint match {
      case Nil =>
        stack match {
          case Nil => Seq.empty
          case top :: remainingStack =>
            recTreeToString(top, prefix.dropRight(4), remainingStack)
        }
      case last :: Nil =>
        Seq(s"$prefix╙──${last.description}") ++
        recTreeToString(last.children.toList, s"$prefix    ", Nil :: stack)
      case next :: siblings =>
        Seq(s"$prefix╟──${next.description}") ++
        recTreeToString(next.children.toList, s"$prefix║   ", siblings :: stack)
    }
  }
}
