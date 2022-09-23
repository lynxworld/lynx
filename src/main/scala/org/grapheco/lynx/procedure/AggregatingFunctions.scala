package org.grapheco.lynx.procedure

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxNull, LynxNumber}
import org.grapheco.lynx.types.time.LynxDuration

import java.time.Duration

/**
 * @ClassName AggregatingFunctions
 * @Description These functions take multiple values as arguments, and
 *  calculate and return an aggregated value from them.
 * @Author Hu Chuan
 * @Date 2022/4/20
 * @Version 0.1
 */
class AggregatingFunctions {
  /**
   * Returns the average of a set of numeric values
   * Considerations:
   * - avg(null) returns null
   * @param inputs An expression returning a set of numeric values.
   * @return Either an Integer or a Float, depending on the values
   *         returned by expression and whether or not the calculation overflows
   */
  @LynxProcedure(name = "avg")
  def avg(inputs: LynxList): LynxValue = {
    val dropNull = inputs.value.filterNot(LynxNull.equals)
    val firstIsNum = dropNull.headOption.map{
      case _ :LynxNumber => true
      case _ :LynxDuration => false
      case v :LynxValue => throw ProcedureException(s"avg() can only handle numerical values, duration, and null. Got ${v}")
    }
    if (firstIsNum.isDefined) {
      var numSum = 0.0
      var durSum = Duration.ZERO
      dropNull.foreach{ v =>
        if (v.isInstanceOf[LynxNumber] || v.isInstanceOf[LynxDuration]) {
          if (v.isInstanceOf[LynxNumber] == firstIsNum.get) {
            if (firstIsNum.get) { numSum += v.asInstanceOf[LynxNumber].number.doubleValue()}
            else { durSum = durSum.plus(v.asInstanceOf[LynxDuration].duration)}
          } else { throw ProcedureException("avg() cannot mix number and duration")}
        } else { throw ProcedureException(s"avg() can only handle numerical values, duration, and null. Got ${v}")}
      }
      if (firstIsNum.get) { LynxFloat(numSum / dropNull.length)}
      else { LynxDuration(durSum.dividedBy(dropNull.length))}
    } else { LynxNull }
  }

  /**
   * Returns a list containing the values returned by an expression.
   * Using this function aggregates data by amalgamating multiple records or values into a single list
   * @param inputs An expression returning a set of values.
   * @return A list containing heterogeneous elements;
   *         the types of the elements are determined by the values returned by expression.
   */
  @LynxProcedure(name = "collect")
  def collect(inputs: LynxList): LynxList = { //TODO other considerations
    inputs
  }

  /**
   * Returns the number of values or records, and appears in two variants:
   * @param inputs An expression
   * @return An Integer
   */
  @LynxProcedure(name = "count") //TODO count() is complex
  def count(inputs: LynxList): LynxInteger = {
    LynxInteger(inputs.value.length)
  }

  /**
   * Returns the maximum value in a set of values.
   * @param inputs An expression returning a set containing any combination of property types and lists thereof.
   * @return A property type, or a list, depending on the values returned by expression.
   */
  @LynxProcedure(name = "max")
  def max(inputs: LynxList): LynxValue = {
    inputs.max
  }

  /**
   * Returns the minimum value in a set of values.
   * @param inputs An expression returning a set containing any combination of property types and lists thereof.
   * @return A property type, or a list, depending on the values returned by expression.
   */
  @LynxProcedure(name = "min")
  def min(inputs: LynxList): LynxValue = {
    inputs.min
  }

  /**
   *  returns the percentile of the given value over a group, with a percentile from 0.0 to 1.0.
   *  It uses a linear interpolation method, calculating a weighted average between two values if
   *  the desired percentile lies between them. For nearest values using a rounding method, see percentileDisc.
   * @param inputs A numeric expression.
   * @param percentile A numeric value between 0.0 and 1.0
   * @return A Double.
   */
  @LynxProcedure(name = "percentileCont")
  def percentileCont(inputs: LynxList, percentile: LynxFloat): LynxFloat ={ // TODO implement it.
    if(percentile.value>1||percentile.value<0){
      throw ProcedureException("percentile should be in range 0 to 1\n")
    }
    val inputFilter = inputs.value.filter(e=>e!=LynxNull).map(e=>e.value.toString.toFloat).sorted
    val x:Double = 1+(inputFilter.length-1)*percentile.value
    val k:Int = math.floor(x).toInt
    LynxFloat(inputFilter(k-1)+(x-k)*(inputFilter(k)-inputFilter(k-1)))
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
    val firstIsNum = dropNull.headOption.map{
      case _ :LynxNumber => true
      case _ :LynxDuration => false
      case v :LynxValue => throw ProcedureException(s"sum() can only handle numerical values, duration, and null. Got ${v}")
    }
    if (firstIsNum.isDefined) {
      var numSum = 0.0
      var durSum = Duration.ZERO
      dropNull.foreach{ v =>
        if (v.isInstanceOf[LynxNumber] || v.isInstanceOf[LynxDuration]) {
          if (v.isInstanceOf[LynxNumber] == firstIsNum.get) {
            if (firstIsNum.get) { numSum += v.asInstanceOf[LynxNumber].number.doubleValue()}
            else { durSum = durSum.plus(v.asInstanceOf[LynxDuration].duration)}
          } else { throw ProcedureException("sum() cannot mix number and duration")}
        } else { throw ProcedureException(s"sum() can only handle numerical values, duration, and null. Got ${v}")}
      }
      if (firstIsNum.get) LynxFloat(numSum) else LynxDuration(durSum)
    } else { LynxNull }
  }

//  @LynxProcedure(name = "stDev")
//  def stDev(): Unit ={
//
//  }
//
//  @LynxProcedure(name = "stDevP")
//  def stDevP(): Unit ={
//
//  }
//
//  @LynxProcedure(name = "percentileDisc")
//  def percentileDisc(): Unit ={
//
//  }


}
