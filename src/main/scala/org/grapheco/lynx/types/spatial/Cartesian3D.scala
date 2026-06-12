package org.grapheco.lynx.types.spatial

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.LynxPropertyKey

case class Cartesian3D(x: LynxFloat, y: LynxFloat, z: LynxFloat) extends LynxPoint {
  override val crs: LynxString = LynxString("cartesian-3d")

  override val srid: LynxInteger = LynxInteger(9157)

  override def keys: Seq[LynxPropertyKey] = super.keys :+ LynxPropertyKey("z")

  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = propertyKey.value match {
    case "z" => Some(this.z)
    case _ => super.property(propertyKey)
  }

  override def toString: String = s"point({x: ${x}, y: ${y}, z: ${z}, crs: '${crs}'})"
}
