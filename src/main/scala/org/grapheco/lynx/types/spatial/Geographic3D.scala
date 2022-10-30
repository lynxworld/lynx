package org.grapheco.lynx.types.spatial

import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxString}

case class Geographic3D(x: LynxFloat, y: LynxFloat, z: LynxFloat) extends LynxPoint {

  val latitude: LynxFloat = y

  val longitude: LynxFloat = x

  val height: LynxFloat = z

  override val crs: LynxString = LynxString("wgs-84-3d")

  override val srid: LynxInteger = LynxInteger(4979)

  override def toString: String = s"point({x: ${x}, y: ${y}, z: ${z}, crs: '${crs}'})"
}
