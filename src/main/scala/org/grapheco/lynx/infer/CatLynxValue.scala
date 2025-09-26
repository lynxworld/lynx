package org.grapheco.lynx.infer

import org.grapheco.lynx.types.{LynxType, LynxValue}

case class ProbLynxValue(value: LynxValue, prob: Double) extends LynxValue {
  override def lynxType: LynxType = value.lynxType
}

object ProbLynxValue {
  def apply(value: Any, prob: Double): ProbLynxValue = new ProbLynxValue(LynxValue(value), prob)
}

class CatLynxValue(values: List[ProbLynxValue]) extends LynxValue {
  override def value: Any = values

  override def lynxType: LynxType = values.head.lynxType

  def best: LynxValue = values.maxBy(_.prob)
}

object CatLynxValue {
  def empty(ty: LynxType): CatLynxValue = new CatLynxValue(List())

  def apply(values: List[ProbLynxValue]): CatLynxValue = {
    val selected = selector.select(values.map(v => v -> v.prob))
    new CatLynxValue(selected.map(_._1))
  }

  def selector: ResultSelector = MEDSelector
}

trait ResultSelector {
  def select[T](result: List[(T, Double)]): List[(T, Double)]
}

object Top1Selector extends ResultSelector {
  override def select[T](result: List[(T, Double)]): List[(T, Double)] = {
    if (result.isEmpty) return List()
    List(result.maxBy(_._2))
  }
}

object MEDSelector extends ResultSelector {

  override def select[T](result: List[(T, Double)]): List[(T, Double)] = {
    val pick = result.sortBy(_._2).reverse
    val i = findOptimalPartition(pick.map(_._2).toArray)
    pick.slice(0, i)
  }

  private def log2(x: Double): Double = math.log(x) / math.log(2)

  private def calculateEntropy(probabilities: Array[Double]): Double = {
    if (probabilities.isEmpty) return 0.0
    val sum = probabilities.sum
    probabilities.map(p => p / sum).map(p => - p * log2(p)).sum
  }

  private def findOptimalPartition(frequencies: Array[Double]): Int = {
    val totalProb = frequencies.sum
    var minEntropy = Double.MaxValue
    var optimalIndex = -1

    // 计算前缀和
    val prefixSum = new Array[Double](frequencies.length + 1)
    for (i <- frequencies.indices) {
      prefixSum(i + 1) = prefixSum(i) + frequencies(i)
    }

    // 遍历可能的分界点
    for (i <- 2 to frequencies.length) {
      val leftProb = prefixSum(i)
      val rightProb = totalProb - leftProb

      val leftEntropy = calculateEntropy(frequencies.slice(0, i))
      val rightEntropy = calculateEntropy(frequencies.slice(i, frequencies.length))

      val totalEntropy = leftProb * leftEntropy + rightProb * rightEntropy
      println(s"${frequencies.slice(0, i).toList} | ${frequencies.slice(i, frequencies.length).toList}")
      println(s"leftEntropy: $leftEntropy, rightEntropy: $rightEntropy, totalEntropy: $totalEntropy")

      if (totalEntropy < minEntropy) {
        minEntropy = totalEntropy
        optimalIndex = i
      }
    }

    optimalIndex
  }
}

