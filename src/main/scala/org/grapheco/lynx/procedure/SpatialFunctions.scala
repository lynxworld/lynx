package org.grapheco.lynx.procedure

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.{LynxFloat, LynxNull}
import org.grapheco.lynx.types.spatial.{Cartesian2D, Cartesian3D, DistanceCalculator, Geographic2D, Geographic3D, LynxPoint, SpatialHelper}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 16:05 2022/10/25
 * @Modified By:
 */
class SpatialFunctions {

  @LynxProcedure(name = "distance")
  def distance(point1: LynxPoint, point2: LynxPoint): LynxValue = distanceOfPoints(point1, point2)

  @LynxProcedure(name = "point.distance")
  def distanceAlias(point1: LynxPoint, point2: LynxPoint): LynxValue = distanceOfPoints(point1, point2)

  @LynxProcedure(name = "point")
  def point(map: LynxMap): LynxPoint = {
    SpatialHelper.parse(map)
  }

  private def distanceOfPoints(point1: LynxPoint, point2: LynxPoint): LynxValue = (point1, point2) match {
    case (a: Cartesian2D, b: Cartesian2D) => DistanceCalculator.ofCartesian2D(a, b)
    case (a: Cartesian3D, b: Cartesian3D) => DistanceCalculator.ofCartesian3D(a, b)
    case (a: Geographic2D, b: Geographic2D) => DistanceCalculator.ofGeographic2D(a, b)
    case (a: Geographic3D, b: Geographic3D) => DistanceCalculator.ofGeographic3D(a, b)
    case _ => LynxNull
  }

}
