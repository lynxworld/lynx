package org.grapheco.lynx.types.spatial

import org.grapheco.lynx.types.property.LynxFloat
import java.lang.Math._

object DistanceCalculator {
  val EARTH_RADIUS_METERS = 6378140.0
  private val EXTENSION_FACTOR = 1.0001

  def ofCartesian2D(a: Cartesian2D, b: Cartesian2D): LynxFloat =
    LynxFloat(pythagoras(Seq(a.x.v, a.y.v), Seq(b.x.v, b.y.v)))

  def ofCartesian3D(a: Cartesian3D, b: Cartesian3D): LynxFloat =
    LynxFloat(pythagoras(Seq(a.x.v, a.y.v, a.z.v), Seq(b.x.v, b.y.v, a.z.v)))

  def ofGeographic2D(a: Geographic2D, b: Geographic2D): LynxFloat =
    LynxFloat(EARTH_RADIUS_METERS * greatCircleDistance(a.x.v, b.x.v, a.y.v, b.y.v))

  def ofGeographic3D(a: Geographic3D, b: Geographic3D): LynxFloat = {
    val avgHeight = (a.z.v + b.z.v) / 2
    val distance2D = (EARTH_RADIUS_METERS + avgHeight) * greatCircleDistance(a.x.v, b.x.v, a.y.v, b.y.v)
    LynxFloat(pythagoras(Seq(distance2D, 0), Seq(0, a.z.v - b.z.v)))
  }

  private def pythagoras(a: Seq[Double], b: Seq[Double]): Double =
    sqrt(a.zip(b).map{case(ai, bi) => pow(ai - bi, 2)}.sum)

  private def greatCircleDistance(x1: Double, x2: Double, y1: Double, y2: Double): Double = {
    val x_1 = toRadians(x1)
    val x_2 = toRadians(x2)
    val y_1 = toRadians(y1)
    val y_2 = toRadians(y2)
    val dx = x_2 - x_1
    val dy = y_2 - y_1
    val alpha = pow(sin(dy / 2), 2.0) + cos(y_1) * cos(y_2) * pow(sin(dx / 2.0), 2.0)
    2.0 * atan2(sqrt(alpha), sqrt(1 - alpha))
  }


}
