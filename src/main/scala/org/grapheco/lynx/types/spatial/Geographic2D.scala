package org.grapheco.lynx.types.spatial

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.LynxPropertyKey

case class Geographic2D(x: LynxFloat, y: LynxFloat) extends LynxPoint {

  val latitude: LynxFloat = y

  val longitude: LynxFloat = x

  override val crs: LynxString = LynxString("wgs-84")

  override val srid: LynxInteger = LynxInteger(4326)

  override def keys: Seq[LynxPropertyKey] = super.keys ++ Seq("latitude", "longitude").map(LynxPropertyKey)

  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = propertyKey.value match {
    case "latitude" => Some(this.latitude)
    case "longitude" => Some(this.longitude)
    case _ => super.property(propertyKey)
  }

  override def toString: String = s"point({x: ${x}, y: ${y}, crs: '${crs}'})"
}
