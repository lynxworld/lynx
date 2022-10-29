package org.grapheco.lynx.types.spatial

import org.grapheco.lynx.procedure.ProcedureException
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.{LynxInteger, LynxNumber, LynxString}
import org.grapheco.lynx.types.spatial.LynxPoint

object SpatialHelper {
  def parse(map: LynxMap): LynxPoint= {
    val crs  = map.get("crs")
    val srid = map.get("srid")

    val longitude = map.get("longitude")
    val latitude  = map.get("latitude")
    val height    = map.get("height")

    val x = map.get("x")
    val y = map.get("y")
    val z = map.get("z")

    val whichType = (crs, srid) match {
      case (Some(LynxString(name)),Some(LynxInteger(id))) => throw ProcedureException("Cannot specify both CRS and SRID")
      case (Some(LynxString(name)), None) => LynxPoint.crsNames.getOrElse(name.toLowerCase(), throw ProcedureException("Unknown coordinate reference system: " + name))
      case (None, Some(LynxInteger(id))) => LynxPoint.srids.getOrElse(id.toInt, throw ProcedureException("Unknown coordinate reference system code: " + id))
      case (None, None) => (longitude.isDefined && latitude.isDefined, height.isDefined || z.isDefined ) match {
        case (true,  true)  => SpatialType.WGS_84_3D
        case (true,  false) => SpatialType.WGS_84_2D
        case (false, _)  => if(z.isDefined) SpatialType.Cartesian3D else SpatialType.Cartesian2D
      }
      case (_, _) => throw ProcedureException("wrong value of coordinate reference system or code.")
    }
    whichType match {
      case SpatialType.WGS_84_2D => {
        (x.isDefined, y.isDefined, longitude.isDefined, latitude.isDefined) match {
          case (true, true, _, _) => (x,y)
          case (_, _, true, true) => (longitude, latitude)
          case (_, _, _, _) => throw ProcedureException("...")
        }
      } match {
        case (Some(x:LynxNumber), Some(y: LynxNumber)) => Geographic2D(x.toLynxFloat, y.toLynxFloat)
        case (_, _) => throw ProcedureException("...")
      }
      case SpatialType.WGS_84_3D => {
        (x.isDefined, y.isDefined, longitude.isDefined, latitude.isDefined) match {
          case (true, true, _, _) => (x, y, Some(height.getOrElse(z.get)))
          case (_, _, true, true) => (longitude, latitude, Some(height.getOrElse(z.get)))
          case (_, _, _, _) => throw ProcedureException("...")
        }
      }match {
        case (Some(x: LynxNumber), Some(y: LynxNumber), Some(z: LynxNumber)) => Geographic3D(x.toLynxFloat, y.toLynxFloat, z.toLynxFloat)
        case (_, _, _) => throw ProcedureException("...")
      }
      case SpatialType.Cartesian2D => (x, y) match {
        case (Some(x: LynxNumber), Some(y: LynxNumber)) => Cartesian2D(x.toLynxFloat, y.toLynxFloat)
        case (_, _) => throw ProcedureException("...")
      }
      case SpatialType.Cartesian3D => (x, y, z) match {
        case (Some(x: LynxNumber), Some(y: LynxNumber), Some(z: LynxNumber)) => Cartesian3D(x.toLynxFloat, y.toLynxFloat, z.toLynxFloat)
        case (_, _, _) => throw ProcedureException("...")
      }
    }
  }

}
