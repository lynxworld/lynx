package org.grapheco.lynx.infer.cache.core

import scala.collection.mutable

object Dep {
  val _dep: mutable.Map[Int, Double] = mutable.Map()

  val _depGraph: mutable.Map[Int, Set[Int]] = mutable.Map()

  def getDep(id: Int): Double = _dep.getOrElseUpdate(id, 1)

  def setDep(id: Int, dep: Double): Unit = _dep(id) = dep

}
