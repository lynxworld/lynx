package org.grapheco.lynx.procedure.functions

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.procedure.exceptions.LynxProcedureException
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxNull, LynxString}
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}

import java.util.regex.Pattern

/**
 * @ClassName ScalarFunctions
 * @Description These functions return a single value.
 * @Author Hu Chuan
 * @Date 2022/4/20
 * @Version 0.1
 */
class ScalarFunctions {

  val booleanPattern = Pattern.compile("true|false", Pattern.CASE_INSENSITIVE)
  val numberPattern = Pattern.compile("-?[0-9]+.?[0-9]*")

  //  @LynxProcedure(name = "coalesce")
  //  def coalesce()

  //  /**
  //   * Returns the end node of a relationship.
  //   * Considerations:
  //   *   endNode(null) returns null
  //   * @param relationship An expression that returns a relationship.
  //   * @return A Node.
  //   */
  //  @LynxProcedure(name = "endNode")
  //  def endNode(relationship: LynxRelationship): LynxNode = {
  //
  //  }


  /**
   * Returns the first element in a list.
   * Considerations:
   * - head(null) returns null.
   * - If the first element in list is null, head(list) will return null.
   * @param list An expression that returns a list.
   * @return The type of the value returned will be that of the first element of list.
   */
  @LynxProcedure(name = "head")
  def head(list: LynxList): LynxValue = {
    list.v.headOption.getOrElse(LynxNull)
  }

  /**
   * Returns the id of a relationship or node.
   * Considerations:
   * - id(null) returns null.
   * @param x An expression that returns a node or a relationship.
   * @return An Integer
   */
  @LynxProcedure(name = "id")
  def id(x: LynxValue): LynxInteger = x match {
    case n: LynxNode => n.id.toLynxInteger
    case r: LynxRelationship => r.id.toLynxInteger
    case _ => throw LynxProcedureException("id() can only used on node or relationship.")
  }

  /**
   * Returns the last element in a list.
   * Considerations:
   * - last(null) returns null.
   * - If the last element in list is null, last(list) will return null.
   * @param list An expression that returns a list.
   * @return The type of the value returned will be that of the last element of list.
   */
  @LynxProcedure(name = "last")
  def last(list: LynxList): LynxValue = {
    list.v.lastOption.getOrElse(LynxNull)
  }

  /**
   * Returns the length of a path.
   * Considerations:
   * - length(null) returns null.
   * @param path An expression that returns a path.
   * @return An Integer
   */
  @LynxProcedure(name = "length")
  def length(path: LynxList): LynxInteger = { //fixme how to calculate the length of a path
    LynxInteger(path.v.size)
  }

  /**
   * Returns a map containing all the properties of a node or relationship.
   * If the argument is already a map, it is returned unchanged.
   * Consideration:
   * - properties(null) returns null
   * @param x An expression that returns a node, a relationship, or a map.
   * @return A Map
   */
  @LynxProcedure(name = "properties")
  def properties(x: LynxValue): LynxMap = x match {
    case n: LynxNode => LynxMap(n.keys.map( k => k.value -> n.property(k).getOrElse(LynxNull)).toMap)
    case r: LynxRelationship => LynxMap(r.keys.map( k => k.value -> r.property(k).getOrElse(LynxNull)).toMap)
    case m: LynxMap => m
    case _ => throw LynxProcedureException("properties() can only used on node, relationship or map.")
  }

  /**
   * Returns the number of elements in a list.
   * Considerations:
   * - size(null) returns null.
   * @param list An expression that returns a list.
   * @return An Integer.
   */
  @LynxProcedure(name = "size")
  def size(list: LynxList): LynxInteger = {
    LynxInteger(list.value.size)
  }

  // TODO size() applied to pattern expression

  /**
   * size() applied to string: returns the size of a string value
   * @param string An expression that returns a string value.
   * @return An Integer.
   */
  @LynxProcedure(name = "size")
  def size(string: LynxString): LynxInteger = {
    LynxInteger(string.value.length)
  }

  //  @LynxProcedure(name = "startNode")
  //  def startNode(lynxRelationship: LynxRelationship): LynxNode = {
  //
  //  }

  /**
   * Will return the same value during one entire query, even for long-running queries.
   * TODO how to ensure same value during one entire query?
   * @return An Integer
   */
  @LynxProcedure(name = "timestamp")
  def timestamp(): LynxInteger = {
    LynxInteger(System.currentTimeMillis())
  }

  /**
   * Converts a string value to a boolean value.
   * Considerations:
   * - toBoolean(null returns null.
   * - if expression is a boolean value, it will be returned unchanged.
   * - if the parsing fails, null will be returned.
   * @param x An expression that returns a boolean or string value.
   * @return A Boolean
   */
  @LynxProcedure(name = "toBoolean")
  def toBoolean(x: LynxValue): LynxValue = {
    x match {
      case LynxString(str) => {
        val res = booleanPattern.matcher(str)
        if (res.matches()) LynxValue(str.toBoolean)
        else LynxNull
      }
      case b: LynxBoolean => b
      case _ => throw LynxProcedureException("toBoolean conversion failure")
    }
  }

  /**
   * Converts an integer or string value to a floating point number.
   * Considerations:
   * - toFloat(null) returns null
   * - If expression is a floating point number, it will be returned unchanged.
   * - If the parsing fails, null will be returned
   * @param x An expression that returns a numeric or string value.
   * @return A Float
   */
  @LynxProcedure(name = "toFloat")
  def toFloat(x: LynxValue): LynxValue = {
    x match {
      case d: LynxFloat => d
      case i: LynxInteger => LynxFloat(i.number.doubleValue())
      case r: LynxString => {
        val str = r.value
        val res = numberPattern.matcher(str)
        if (res.matches()) LynxFloat(str.toDouble)
        else LynxNull
      }
      case _ => throw LynxProcedureException("toFloat conversion failure")
    }
  }

  /**
   * Converts a floating point or string value to an integer value
   * COnsiderations:
   * - toInteger(null) returns null.
   * - If expression is an integer value, it will be returned unchanged.
   * - If the parsing fails, null will be returned.
   * @param x An expression that returns a numeric or string value.
   * @return An Integer.
   */
  @LynxProcedure(name = "toInteger")
  def toInteger(x: LynxValue): LynxValue = {
    x match {
      case i: LynxInteger => i
      case d: LynxFloat => LynxInteger(d.number.intValue())
      case LynxString(str) =>
        if (numberPattern.matcher(str).matches()) LynxInteger(str.toDouble.toInt)
        else LynxNull
      case _ => throw LynxProcedureException("toInteger conversion failure")
    }
  }

  /**
   * Returns the string representation of the relationship type.
   * @param x An expression that returns a relationship.
   * @return A String
   */
  @LynxProcedure(name = "type")
  def getType(x: LynxRelationship): LynxString = {
    x.relationType.map(_.value).map(LynxString).getOrElse(LynxString(""))
    //    x match {
    //      case r: LynxRelationship => {
    //        val t = r.relationType
    //        if (t.isDefined) LynxValue(t.get)
    //        else LynxNull
    //      }
    //      case _ => throw new LynxProcedureException("type can only used on relationship")
    //    }
  }

}
