package org.grapheco.lynx.types.spatial

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.LynxPropertyKey

case class Geographic3D(x: LynxFloat, y: LynxFloat, z: LynxFloat) extends LynxPoint {

  val latitude: LynxFloat = y

  val longitude: LynxFloat = x

  val height: LynxFloat = z

  override val crs: LynxString = LynxString("wgs-84-3d")

  override val srid: LynxInteger = LynxInteger(4979)

  override def keys: Seq[LynxPropertyKey] = super.keys ++ Seq("latitude", "longitude", "height", "z").map(LynxPropertyKey)

  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = propertyKey.value match {
    case "latitude" => Some(this.latitude)
    case "longitude" => Some(this.longitude)
    case "height" => Some(this.height)
    case "z" => Some(this.z)
    case _ => super.property(propertyKey)
  }

  override def toString: String = s"point({x: ${x}, y: ${y}, z: ${z}, crs: '${crs}'})"
}
