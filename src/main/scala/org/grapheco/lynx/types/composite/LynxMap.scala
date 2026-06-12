package org.grapheco.lynx.types.composite

import org.grapheco.lynx.types.{LTMap, LynxValue, MapType}
import org.grapheco.lynx.types.structural.LynxPropertyKey
import org.grapheco.lynx.types.traits.HasProperty

/**
 * @ClassName LynxMap
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxMap(v: Map[String, LynxValue]) extends LynxCompositeValue with HasProperty {
  override def value: Map[String, LynxValue] = v

  override def lynxType: MapType = LTMap

  // TODO: map comparability
  override def sameTypeCompareTo(o: LynxValue): Int = {0}

  def get(key: String): Option[LynxValue] = value.get(key)

  override def keys: Seq[LynxPropertyKey] = v.keys.map(LynxPropertyKey).toSeq

  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = v.get(propertyKey.value)
}
