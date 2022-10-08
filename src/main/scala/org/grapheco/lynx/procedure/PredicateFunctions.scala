package org.grapheco.lynx.procedure

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxBoolean, LynxString}

/**
 * @ClassName PredicateFunctions
 * @Description These functions return either true or false for the given arguments.
 * @Author huchuan
 * @Date 2022/4/20
 * @Version 0.1
 */
class PredicateFunctions {
  /**
   * Returns true if the specified property exists in the node, relationship or map.
   *
   * @param property A property (in the form 'variable.prop')
   * @return A Boolean
   */
  @LynxProcedure(name = "exists")
  def exists(property: LynxValue): LynxBoolean = {
    property match {
      case list: LynxList => LynxBoolean(list.value.nonEmpty) // TODO how to judge a list?
      case b: LynxBoolean => b
      case _ => LynxBoolean(property.value != null)
    }
  }

  @LynxProcedure(name = "isEmpty")
  def isEmpty(input: LynxValue): LynxBoolean = {
    input match {
      case list:LynxList => LynxBoolean(list.v.isEmpty)
      case s:LynxString => LynxBoolean(s.value.isEmpty)
    }
  }


  @LynxProcedure(name = "single")
  def single(input: LynxValue): LynxBoolean = {
    LynxBoolean(true)
  }



}
