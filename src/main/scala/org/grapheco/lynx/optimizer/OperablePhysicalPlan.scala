package org.grapheco.lynx.optimizer

import org.grapheco.lynx.physical.plans.PhysicalPlan

class OperablePhysicalPlan(p: PhysicalPlan) {

  def pushDown(rightChild: Boolean = false, childRight: Boolean = false): PhysicalPlan = (rightChild, childRight) match {
    case (false, false) => pushLeftDownLeft
    case (false, true)  => pushLeftDownRight
    case (true, false)  => pushRightDownLeft
    case (true, true)   => pushRightDownRight
  }


  /*
    [P,P.R]
     ║
    [Q]
     ╟──[L]
     ╙──[R]
     ========= To ========
    [Q]
     ╟──[P,P.R]──[L]
     ╙──[R]
   */
  def pushLeftDownLeft: PhysicalPlan = p.left.map{ Q =>
    val L = Q.left
    val R = Q.right
    Q.withChildren(
      Some(p.withChildren(L, p.right)),
      R)
  }.getOrElse(p)

  /*
    [P,P.R]
     ║
    [Q]
     ╟──[L]
     ╙──[R]
     ========= To ========
    [Q]
     ╟──[L]
     ╙──[P,P.R]──[R]
   */
  def pushLeftDownRight: PhysicalPlan = p.left.map { Q =>
    val L = Q.left
    val R = Q.right
    Q.withChildren(
      L,
      Some(p.withChildren(R, p.right)))
  }.getOrElse(p)

  /*
    [P,P.L]
     ║
    [Q]
     ╟──[L]
     ╙──[R]
     ========= To ========
    [Q]
     ╟──[P,P.L]──[L]
     ╙──[R]
   */
  def pushRightDownLeft: PhysicalPlan = p.right.map { Q =>
    val L = Q.left
    val R = Q.right
    Q.withChildren(
      Some(p.withChildren(p.left, L)),
      R)
  }.getOrElse(p)

  /*
    [P,P.L]
     ║
    [Q]
     ╟──[L]
     ╙──[R]
     ========= To ========
    [Q]
     ╟──[L]
     ╙──[P,P.L]──[R]
   */
  def pushRightDownRight: PhysicalPlan = p.right.map { Q =>
    val L = Q.left
    val R = Q.right
    Q.withChildren(
      L,
      Some(p.withChildren(p.left, R)))
  }.getOrElse(p)
}
