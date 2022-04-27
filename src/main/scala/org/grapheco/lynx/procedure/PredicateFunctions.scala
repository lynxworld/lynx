package org.grapheco.lynx.procedure

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList

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
   * @param property A property (in the form 'variable.prop')
   * @return A Boolean
   */
  @LynxProcedure(name = "exists")
  def exists(property: LynxValue): Boolean = {
    property match {
      case list: LynxList => ??? // TODO how to judge a list?
      case _ => property.value != null
    }
  }

}
