package org.grapheco.lynx.procedure

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxNull, LynxNumber}
import org.grapheco.lynx.types.time.LynxDuration
import org.grapheco.lynx.{ProcedureException, LynxNumber}

import java.time.Duration
import scala.math._
import scala.collection.mutable

/**
 * @ClassName AggregatingFunctions
 * @Description These functions take multiple values as arguments, and
 *              calculate and return an aggregated value from them.
 * @Author Hu Chuan
 * @Date 2022/4/20
 * @Version 0.1
 */
class AggregatingFunctions {
  /**
   * Returns the average of a set of numeric values
   * Considerations:
   * - avg(null) returns null
   *
   * @param inputs An expression returning a set of numeric values.
   * @return Either an Integer or a Float, depending on the values
   *         returned by expression and whether or not the calculation overflows
   */
  @LynxProcedure(name = "avg")
  def avg(inputs: LynxList): LynxValue = {
    val dropNull = inputs.value.filterNot(LynxNull.equals)
    val firstIsNum = dropNull.headOption.map {
      case _: LynxNumber => true
      case _: LynxDuration => false
      case v: LynxValue => throw ProcedureException(s"avg() can only handle numerical values, duration, and null. Got ${v}")
    }
    if (firstIsNum.isDefined) {
      var numSum = 0.0
      var durSum = Duration.ZERO
      dropNull.foreach { v =>
        if (v.isInstanceOf[LynxNumber] || v.isInstanceOf[LynxDuration]) {
          if (v.isInstanceOf[LynxNumber] == firstIsNum.get) {
            if (firstIsNum.get) {
              numSum += v.asInstanceOf[LynxNumber].number.doubleValue()
            }
            else {
              durSum = durSum.plus(v.asInstanceOf[LynxDuration].value)
            }
          } else {
            throw ProcedureException("avg() cannot mix number and duration")
          }
        } else {
          throw ProcedureException(s"avg() can only handle numerical values, duration, and null. Got ${v}")
        }
      }
      if (firstIsNum.get) {
        LynxFloat(numSum / dropNull.length)
      }
      else {
        LynxDuration.parse((durSum.dividedBy(dropNull.length)).toString, false)
      }
    } else {
      LynxNull
    }
  }

  /**
   * Returns a list containing the values returned by an expression.
   * Using this function aggregates data by amalgamating multiple records or values into a single list
   *
   * @param inputs An expression returning a set of values.
   * @return A list containing heterogeneous elements;
   *         the types of the elements are determined by the values returned by expression.
   */
  @LynxProcedure(name = "collect")
  def collect(inputs: LynxList): LynxList = { //TODO other considerations
    LynxList(inputs.v.filterNot(LynxNull.equals))
  }

  /**
   * Returns the number of values or records, and appears in two variants:
   *
   * @param inputs An expression
   * @return An Integer
   */
  @LynxProcedure(name = "count") //TODO count() is complex
  def count(inputs: LynxList): LynxInteger = {
    LynxInteger(inputs.value.filterNot(LynxNull.equals).length)
  }

  /**
   * Returns the maximum value in a set of values.
   *
   * @param inputs An expression returning a set containing any combination of property types and lists thereof.
   * @return A property type, or a list, depending on the values returned by expression.
   */
  @LynxProcedure(name = "max")
  def max(inputs: LynxList): LynxValue = {
    inputs.max
  }

  /**
   * Returns the minimum value in a set of values.
   *
   * @param inputs An expression returning a set containing any combination of property types and lists thereof.
   * @return A property type, or a list, depending on the values returned by expression.
   */
  @LynxProcedure(name = "min")
  def min(inputs: LynxList): LynxValue = {
    inputs.min
  }

  /**
   * returns the percentile of the given value over a group, with a percentile from 0.0 to 1.0.
   * It uses a linear interpolation method, calculating a weighted average between two values if
   * the desired percentile lies between them. For nearest values using a rounding method, see percentileDisc.
   *
   * @author along
   * @param inputs     A numeric expression.
   * @param percentile A numeric value between 0.0 and 1.0
   * @return A Double.
   */
  @LynxProcedure(name = "percentileCont")
  def percentileCont(inputs: LynxList, percentile: LynxFloat): LynxFloat = {
    if (percentile.value > 1 || percentile.value < 0) {
      throw ProcedureException("percentile should be in range 0 to 1\n")
    }
    val inputFilter = inputs.value.filterNot(LynxNull.equals).map(e => e.value.toString.toFloat).sorted
    val x: Double = 1 + (inputFilter.length - 1) * percentile.value
    val k: Int = math.floor(x).toInt
    LynxFloat(inputFilter(k - 1) + (x - k) * (inputFilter(k) - inputFilter(k - 1)))
  }

  // percentileDisc, stDev, stDevP

  /**
   *
   * @param inputs
   * @return
   */
  @LynxProcedure(name = "sum")
  def sum(inputs: LynxList): Any = {
    val dropNull = inputs.value.filterNot(LynxNull.equals)
    val firstIsNum = dropNull.headOption.map {
      case _: LynxNumber => true
      case _: LynxDuration => false
      case v: LynxValue => throw ProcedureException(s"sum() can only handle numerical values, duration, and null. Got ${v}")
    }
    if (firstIsNum.isDefined) {
      var numSum = 0.0
      var durSum = Duration.ZERO
      dropNull.foreach { v =>
        if (v.isInstanceOf[LynxNumber] || v.isInstanceOf[LynxDuration]) {
          if (v.isInstanceOf[LynxNumber] == firstIsNum.get) {
            if (firstIsNum.get) {
              numSum += v.asInstanceOf[LynxNumber].number.doubleValue()
            }
            else {
              durSum = durSum.plus(v.asInstanceOf[LynxDuration].value)
            }
          } else {
            throw ProcedureException("sum() cannot mix number and duration")
          }
        } else {
          throw ProcedureException(s"sum() can only handle numerical values, duration, and null. Got ${v}")
        }
      }
      if (firstIsNum.get) LynxFloat(numSum) else LynxDuration.parse(durSum.toString,false)
    } else {
      LynxNull
    }
  }

  /**
   * returns the standard deviation for the given value over a group. It uses a standard two-pass method, with N - 1 as the denominator,
   * and should be used when taking a sample of the population for an unbiased estimate. When the standard variation of the entire population
   * is being calculated, stdDevP should be used.
   *
   * @author along
   * @param inputs
   * @return A Float.
   */
  @LynxProcedure(name = "stDev")
  def stDev(inputs: LynxList): LynxFloat = {
    standardDevUtil(inputs, true)
  }


  /**
   * stDevP() returns the standard deviation for the given value over a group. It uses a standard two- pass method,
   * with N as the denominator, and should be used when calculating the standard deviation for an entire population.
   * When the standard variation of only a sample of the population is being calculated, stDev should be used.
   *
   * @author along
   * @param inputs a numeric list
   * @return A Float.
   */
  @LynxProcedure(name = "stDevP")
  def stDevP(inputs: LynxList): LynxFloat = {
    standardDevUtil(inputs, false)
  }


  /**
   * returns the percentile of the given value over a group, with a percentile from 0.0 to 1.0. It uses a
   * rounding method and calculates the nearest value to the percentile. For interpolated values, see percentileCont.
   *
   * @param inputs     a numeric list
   * @param percentile A numeric value between 0.0 and 1.0
   * @return Either an Integer or a Float, depending on the values returned by expression and whether or not the calculation overflows.
   */
  @LynxProcedure(name = "percentileDisc")
  def percentileDisc(inputs: LynxList, percentile: LynxFloat): LynxValue = {
    val percentileNum = percentileCont(inputs, percentile)
    upBound(inputs, percentileNum)
  }


  /**
   * Tool method for solving standard deviation.
   *
   * @author along
   * @param inputs
   * @param isSample [true] represent sample standard deviation, [false] represent Population standard deviation
   * @return standard deviation
   */
  def standardDevUtil(inputs: LynxList, isSample: Boolean): LynxFloat = {
    val len = inputs.value.length.toFloat
    if (len <= 1) return LynxFloat(0);
    var sum = 0.0
    var res = 0.0
    var avg = 0.0
    val N = if (isSample) len - 1 else len

    inputs.value.foreach(e => sum += e.value.toString.toFloat)
    avg = sum / len
    inputs.value.foreach(e => res += math.pow(e.value.toString.toFloat - avg, 2))
    LynxFloat(math.sqrt(res / N))
  }


  /**
   * Use binary search to find the nearest number to target
   *
   * @author along
   * @param inputs a numeric list
   * @param target a float
   * @return the nearest number to target
   */
  def upBound(inputs: LynxList, target: LynxFloat): LynxValue = {
    val lynxNums = inputs.value.filterNot(LynxNull.equals).sorted
    val nums = lynxNums.map(e => e.value.toString.toFloat)
    var left = 0
    var right = nums.length

    /**
     * search the first number that's greater than or equal to it
     */
    while (left < right) {
      val mid = (left + right) / 2;
      if (nums(mid) == target.value) {
        right = mid
      } else if (nums(mid) < target.value) {
        left = mid + 1
      } else {
        right = mid
      }
    }

    /**
     * nums[left-1]<target<nums[left], Determine which one is nearest to the target
     */
    if (left - 1 >= 0)
      return if (target.value - nums(left - 1) > nums(left) - target.value) lynxNums(left) else lynxNums(left - 1)
    lynxNums(left)
  }


}

/**
 * @ClassName StatisticalFunctions
 * @Description Statistical functions to calculate k-th central moment and k-th raw moment.
 * @ZSJ
 * @Date 2024/12/22
 * @Version 0.1
 */
class StatisticalFunctions {
  /**
   * Calculates the k-th central moment.
   *
   * @param inputs A list of numeric values.
   * @param k      The order of the central moment.
   * @return A Float representing the k-th central moment.
   */
  @LynxProcedure(name = "kthCentralMoment")
  def kthCentralMoment(inputs: LynxList, k: LynxInteger): LynxFloat = {
    val values = inputs.value.filterNot(LynxNull.equals).map(_.asInstanceOf[LynxNumber].number.toDouble)
    if (values.isEmpty) LynxFloat(0) else {
      val mean = values.sum / values.length
      val centralMoment = values.map(x => pow(x - mean, k.number.toDouble)).sum / values.length
      LynxFloat(centralMoment.toFloat)
    }
  }

  /**
   * Calculates the k-th raw moment.
   *
   * @param inputs A list of numeric values.
   * @param k      The order of the raw moment.
   * @return A Float representing the k-th raw moment.
   */
  @LynxProcedure(name = "kthRawMoment")
  def kthRawMoment(inputs: LynxList, k: LynxInteger): LynxFloat = {
    val values = inputs.value.filterNot(LynxNull.equals).map(_.asInstanceOf[LynxNumber].number.toDouble)
    if (values.isEmpty) LynxFloat(0) else {
      val rawMoment = values.map(x => pow(x, k.number.toDouble)).sum / values.length
      LynxFloat(rawMoment.toFloat)
    }
  }
}

/**
 * @ClassName GraphAlgorithms
 * @Add functions to provide graph analysis procedures for a graph database system.
 * @Zr
 * @Date 2024/12/22
 * @Version 0.1
 */

class GraphAlgorithms {

  /**
   * Calculates the PageRank values for nodes in a graph.
   *
   * @param inputs The graph's adjacency list representation, where each element is a LynxList containing a node and its outgoing edges.
   * @return A LynxList containing each node and its corresponding PageRank value.
   */
  @LynxProcedure(name = "pagerank")
  def pagerank(inputs: LynxList): LynxList = {
    val adjacencyList = buildAdjacencyList(inputs)
    val N = adjacencyList.size
    var rank = mutable.Map.empty[String, Double].withDefaultValue(1.0 / N)
    var newRank = mutable.Map.empty[String, Double]
    val damping = 0.85
    val threshold = 1e-6
    var converged = false
    var iterations = 0
    val maxIterations = 100

    while (!converged && iterations < maxIterations) {
      newRank = adjacencyList.keys.map { node =>
        val contrib = adjacencyList(node).map { neighbor =>
          rank(neighbor) / adjacencyList(neighbor).size
        }.sum
        val pr = (1 - damping) / N + damping * contrib
        (node, pr)
      }.toMap
      converged = adjacencyList.keys.forall(node => math.abs(newRank(node) - rank(node)) < threshold)
      rank = newRank
      iterations += 1
    }

    val result = rank.map { case (node, pr) =>
      LynxList(List(LynxString(node), LynxFloat(pr.toFloat)))
    }.toList
    LynxList(result)
  }

  /**
   * Constructs the graph's adjacency list from the input.
   *
   * @param inputs The graph representation, where each element is a LynxList containing a node and its outgoing edges.
   * @return The adjacency list as a Map.
   */
  def buildAdjacencyList(inputs: LynxList): mutable.Map[String, List[String]] = {
    inputs.value.map { nodeEntry =>
      val node = nodeEntry.asInstanceOf[LynxList].value.head.asInstanceOf[LynxString].value
      val neighbors = nodeEntry.asInstanceOf[LynxList].value.tail.head.asInstanceOf[LynxList].value.map(_.asInstanceOf[LynxString].value)
      (node, neighbors)
    }.toMap
  }

  /**
   * Calculates the maximum flow in a flow network using the Edmonds-Karp algorithm.
   *
   * @param inputs A list of edges, where each element is a LynxList containing the start node, end node, and capacity.
   * @param source The source node.
   * @param sink The sink node.
   * @return The value of the maximum flow.
   */
  @LynxProcedure(name = "max_flow")
  def maxFlow(inputs: LynxList, source: LynxString, sink: LynxString): LynxFloat = {
    val edges = inputs.value.map { edgeEntry =>
      val edgeList = edgeEntry.asInstanceOf[LynxList].value
      val from = edgeList(0).asInstanceOf[LynxString].value
      val to = edgeList(1).asInstanceOf[LynxString].value
      val capacity = edgeList(2).asInstanceOf[LynxNumber].number.doubleValue()
      (from, to, capacity)
    }.toList

    val graph = mutable.Map[String, mutable.Map[String, Double]]()
    for ((from, to, capacity) <- edges) {
      if (!graph.contains(from)) graph(from) = mutable.Map.empty
      if (!graph.contains(to)) graph(to) = mutable.Map.empty
      graph(from)(to) = capacity
    }

    def bfs(source: String, sink: String, parent: mutable.Map[String, String]): Boolean = {
      val visited = mutable.Set[String]()
      val queue = mutable.Queue[String]()
      queue.enqueue(source)
      visited.add(source)

      while (queue.nonEmpty) {
        val u = queue.dequeue()
        for ((v, cap) <- graph(u)) {
          if (!visited.contains(v) && cap > 0) {
            visited.add(v)
            parent(v) = u
            if (v == sink) return true
            queue.enqueue(v)
          }
        }
      }
      false
    }

    val parent = mutable.Map.empty[String, String]
    var maxFlow = 0.0

    while (bfs(source.value, sink.value, parent)) {
      var pathFlow = Double.PositiveInfinity
      var v = sink.value
      while (v != source.value) {
        val u = parent(v).get
        pathFlow = math.min(pathFlow, graph(u)(v))
        v = u
      }
      maxFlow += pathFlow
      var v = sink.value
      while (v != source.value) {
        val u = parent(v).get
        graph(u)(v) -= pathFlow
        if (graph(v).contains(u)) {
          graph(v)(u) += pathFlow
        } else {
          graph(v)(u) = pathFlow
        }
        v = parent(v).get
      }
      parent.clear()
    }

    LynxFloat(maxFlow.toFloat)
  }

  /**
   * Detects communities in a graph using the Louvain algorithm.
   *
   * @param inputs The graph's adjacency list representation, where each element is a LynxList containing a node and its outgoing edges.
   * @return A LynxList containing each node and its corresponding community.
   */
  @LynxProcedure(name = "community_detection")
  def communityDetection(inputs: LynxList): LynxList = {
    val adjacencyList = buildAdjacencyList(inputs)
    val nodes = adjacencyList.keys.toList
    var community = mutable.Map.empty[String, Int]
    nodes.zipWithIndex.foreach { case (node, idx) => community(node) = idx }

    var modularity = 0.0
    var newModularity = computeModularity(adjacencyList, community)

    while (newModularity > modularity) {
      modularity = newModularity
      var improved = false
      for (node <- nodes) {
        val currentCommunity = community(node)
        var bestModularityGain = 0.0
        var bestCommunity = currentCommunity
        for (neighbor <- adjacencyList(node)) {
          if (community(neighbor) != currentCommunity) {
            val tempCommunity = community.clone()
            tempCommunity(node) = community(neighbor)
            val tempModularity = computeModularity(adjacencyList, tempCommunity)
            val gain = tempModularity - modularity
            if (gain > bestModularityGain) {
              bestModularityGain = gain
              bestCommunity = community(neighbor)
            }
          }
        }
        if (bestModularityGain > 0) {
          community(node) = bestCommunity
          improved = true
        }
      }
      newModularity = computeModularity(adjacencyList, community)
      if (!improved) {
        break
      }
    }

    val result = community.map { case (node, comm) =>
      LynxList(List(LynxString(node), LynxString(comm.toString)))
    }.toList
    LynxList(result)
  }

  private def computeModularity(adjacencyList: mutable.Map[String, List[String]], community: mutable.Map[String, Int]): Double = {
    val m = adjacencyList.values.flatten.length / 2.0
    var modularity = 0.0
    for (node <- adjacencyList.keys) {
      val k_i = adjacencyList(node).length
      for (neighbor <- adjacencyList(node)) {
        if (community(node) == community(neighbor)) {
          modularity += 1.0 / (2 * m) - math.pow(k_i / (2 * m), 2)
        }
      }
    }
    modularity
  }
}
/**
 * @ClassName Graph
 * @Add Graph algorithm implementation class, including Dijkstra, Prim, and DFS algorithms.
 * @WZR
 * @Date 2024/12/22
 * @Version 0.1
 */

class Graph(val vertices: Int) {
  private val adjList = new Array[mutable.ListBuffer[(Int, Int)]](vertices)

  // Initialize adjacency list
  for (i <- 0 until vertices) {
    adjList(i) = mutable.ListBuffer.empty[(Int, Int)]
  }

  // Add an edge with weight
  def addEdge(v: Int, w: Int, weight: Int): Unit = {
    require(v >= 0 && v < vertices, s"Vertex $v out of bounds")
    require(w >= 0 && w < vertices, s"Vertex $w out of bounds")
    adjList(v) += ((w, weight))
    adjList(w) += ((v, weight)) // for undirected graph
  }

  // Add an unweighted edge with default weight 1
  def addEdge(v: Int, w: Int): Unit = addEdge(v, w, 1)

  // Dijkstra's algorithm for single-source shortest paths
  def dijkstra(src: Int): Array[Int] = {
    val dist = Array.fill(vertices)(Int.MaxValue)
    val visited = new Array[Boolean](vertices)
    dist(src) = 0

    for (i <- 0 until vertices) {
      var u = -1
      var min = Int.MaxValue

      for (v <- 0 until vertices) {
        if (!visited(v) && dist(v) < min) {
          u = v
          min = dist(v)
        }
      }

      if (u == -1) return dist

      visited(u) = true

      for ((neighbor, weight) <- adjList(u)) {
        if (!visited(neighbor) && dist(u) + weight < dist(neighbor)) {
          dist(neighbor) = dist(u) + weight
        }
      }
    }

    dist
  }

  // Prim's algorithm for Minimum Spanning Tree (MST)
  def primMST(): Array[Int] = {
    val key = Array.fill(vertices)(Int.MaxValue)
    val parent = new Array[Int](vertices)
    val inMST = new Array[Boolean](vertices)

    key(0) = 0
    parent(0) = -1

    for (count <- 0 until vertices) {
      var u = -1
      var min = Int.MaxValue

      for (v <- 0 until vertices) {
        if (!inMST(v) && key(v) < min) {
          u = v
          min = key(v)
        }
      }

      if (u == -1) return parent // handle disconnected graphs

      inMST(u) = true

      for ((neighbor, weight) <- adjList(u)) {
        if (!inMST(neighbor) && weight < key(neighbor)) {
          parent(neighbor) = u
          key(neighbor) = weight
        }
      }
    }

    parent
  }

  // Depth-First Search (DFS) traversal
  def dfs(): Unit = {
    val visited = new Array[Boolean](vertices)
    for (i <- 0 until vertices) {
      if (!visited(i)) {
        dfsUtil(i, visited)
      }
    }
  }

  private def dfsUtil(v: Int, visited: Array[Boolean]): Unit = {
    visited(v) = true
    println(v)

    for ((neighbor, _) <- adjList(v)) {
      if (!visited(neighbor)) {
        dfsUtil(neighbor, visited)
      }
    }
  }
}
