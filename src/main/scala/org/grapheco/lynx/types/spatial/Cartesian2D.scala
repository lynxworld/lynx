package org.grapheco.lynx.types.spatial

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxString}

case class Cartesian2D(x: LynxFloat, y: LynxFloat) extends LynxPoint {
  override val crs: LynxString = LynxString("cartesian")

  override val srid: LynxInteger = LynxInteger(7203)

  override def toString: String = s"point({x: ${x}, y: ${y}, crs: '${crs}'})"
}
