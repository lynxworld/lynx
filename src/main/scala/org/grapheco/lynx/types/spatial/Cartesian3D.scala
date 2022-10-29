package org.grapheco.lynx.types.spatial

import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxString}

case class Cartesian3D(x: LynxFloat, y: LynxFloat, z: LynxFloat) extends LynxPoint {
  override val crs: LynxString = LynxString("cartesian-3d")

  override val srid: LynxInteger = LynxInteger(9157)

  override def toString: String = s"point({x: ${x}, y: ${y}, z: ${z}, crs: '${crs}'})"
}
