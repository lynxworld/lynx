package org.grapheco.lynx.types.spatial

import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxString}

case class Geographic2D(x: LynxFloat, y: LynxFloat) extends LynxPoint {

  val latitude: LynxFloat = y

  val longitude: LynxFloat = x

  override val crs: LynxString = LynxString("wgs-84")

  override val srid: LynxInteger = LynxInteger(4326)

  override def toString: String = s"point({x: ${x}, y: ${y}, crs: '${crs}'})"
}
