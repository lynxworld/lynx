package org.grapheco.lynx.types.spatial

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.procedure.ProcedureException
import org.grapheco.lynx.runner.ParsingException
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxNull, LynxString}
import org.grapheco.lynx.types.spatial.SpatialType.SpatialType
import org.opencypher.v9_0.util.symbols.CTPoint

trait LynxPoint extends LynxValue{
  val x: LynxFloat

  val y: LynxFloat

  val crs: LynxString

  val srid: LynxInteger

  override def lynxType: LynxType = CTPoint

  override def value: Any = this

  override def sameTypeCompareTo(o: LynxValue): Int = ???
}
object SpatialType extends Enumeration {
  type SpatialType = Value
  val WGS_84_2D, WGS_84_3D, Cartesian2D, Cartesian3D = Value
}
object LynxPoint{

  val crsNames: Map[String, SpatialType] = Map(
    "wgs-84" -> SpatialType.WGS_84_2D,
    "wgs-84-3d" -> SpatialType.WGS_84_3D,
    "cartesian" -> SpatialType.Cartesian2D,
    "cartesian-3d" -> SpatialType.Cartesian3D)

  val srids: Map[Int, SpatialType] = Map(
    4326 -> SpatialType.WGS_84_2D,
    4979 -> SpatialType.WGS_84_3D,
    7203 ->SpatialType.Cartesian2D,
    9157 ->SpatialType.Cartesian3D)


}

