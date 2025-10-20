package org.grapheco.lynx.infer

import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.{LynxType, LynxValue, TypeCompareException}

case class ProbLynxValue(label: LynxValue, prob: Double) extends LynxValue {

  override def value: Any = label.value

  override def lynxType: LynxType = label.lynxType

  override def compareTo(o: LynxValue): Int = o match {
    case p: ProbLynxValue => label.compareTo(p.label)
    case _ => label.compareTo(o)
  }
}

object ProbLynxValue {
  def none: ProbLynxValue = ProbLynxValue(LynxNull, 1.0)

  def apply(value: Any, prob: Double): ProbLynxValue = new ProbLynxValue(LynxValue(value), prob)
}

class CatLynxValue(val values: List[ProbLynxValue], val certainly: Boolean = false) extends LynxValue {
  override def value: Any = values

  override def lynxType: LynxType = values.headOption.getOrElse(ProbLynxValue.none).lynxType

  def best: ProbLynxValue = values.headOption.getOrElse(ProbLynxValue.none)

  override def toString: String = s"?(${values.map(v => s"${v.value}@${v.prob.toString.take(4)}").mkString(", ")})"

  override def compareTo(o: LynxValue): Int = {
    o match {
      case c: CatLynxValue =>
        if (certainly && c.certainly) return best.compareTo(c.best)
        val compared = for {
          v1 <- values
          v2 <- c.values
        } yield v1.compareTo(v2)
        if (compared.contains(0)) 0
        else compared.headOption.getOrElse(1)
      case _ =>
        if (certainly) return best.compareTo(o)
        val compared = values.map(_.compareTo(o))
        if (compared.contains(0)) 0
        else compared.head
    }
  }
}

object CatLynxValue {
  def empty: CatLynxValue = new CatLynxValue(List(ProbLynxValue.none), certainly = true)

  def apply(values: List[ProbLynxValue]): CatLynxValue = {
    if (values.isEmpty) return empty
    val selected = selector.select(values.map(v => v -> v.prob).sortBy(- _._2)) //desc
    new CatLynxValue(selected.map(_._1), selected.size==1)
  }

  def selector: ResultSelector = MEDSelector
}

trait ResultSelector {
  def select[T](result: List[(T, Double)]): List[(T, Double)]
}

object Top1Selector extends ResultSelector {
  override def select[T](result: List[(T, Double)]): List[(T, Double)] = {
    if (result.isEmpty) return List()
    List(result.head)
  }
}

object MEDSelector extends ResultSelector {

  override def select[T](result: List[(T, Double)]): List[(T, Double)] = {
//    val pick = result.sortBy(_._2).reverse
    val i = findOptimalPartition(result.map(_._2).toArray)
    result.slice(0, i)
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
    for (i <- 1 to frequencies.length) {
      val leftProb = prefixSum(i)
      val rightProb = totalProb - leftProb
//      val leftProb = i
//      val rightProb = frequencies.length - leftProb
      val leftEntropy = calculateEntropy(frequencies.slice(0, i))
      val rightEntropy = calculateEntropy(frequencies.slice(i, frequencies.length))

      val totalEntropy = leftProb * leftEntropy + rightProb * rightEntropy
      if (totalEntropy < minEntropy) {
        minEntropy = totalEntropy
        optimalIndex = i
      }
    }

    optimalIndex
  }
}

