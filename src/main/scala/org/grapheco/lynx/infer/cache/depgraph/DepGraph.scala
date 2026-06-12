//package org.grapheco.lynx.infer.cache.depgraph
//
//// typo: 0 - node, 1 - expand, 2 - prop, 3 - link
//case class DepNode(code: Int, typo: Int, weight: Double, cost: Double)
//
//class DepGraph extends {
//  private val adjList = scala.collection.mutable.Map[DepNode, List[DepNode]]()
//
//  private val nodes = scala.collection.mutable.Map[Int, DepNode]()
//
//  def getNode(code: Int): DepNode = {
//    nodes.getOrElseUpdate(code, DepNode(code, 0, weight = 1.0, cost = 0))
//  }
//
//  def addEdge(fromCode: Int, toCode: Int): Unit = {
//    val fromNode = getNode(fromCode)
//    val toNode = getNode(toCode)
//    addEdge(fromNode, toNode)
//  }
//
//  def addEdge(from: DepNode, to: DepNode): Unit = {
//    val neighbors = adjList.getOrElse(from, List())
//    adjList(from) = to :: neighbors
//  }
//
//  def getOutNeighbors(code: Int): List[DepNode] = {
//    val node = getNode(code)
//    getOutNeighbors(node)
//  }
//
//  def getOutNeighbors(node: DepNode): List[DepNode] = {
//    adjList.getOrElse(node, List())
//  }
//
//  def clear(): Unit = {
//    adjList.clear()
//  }
//}
//
//object DepGraph {
//  def empty: DepGraph = new DepGraph()
//}
