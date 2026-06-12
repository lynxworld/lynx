package org.grapheco.lynx.types.spatial
// Each point will also be associated with a specific Coordinate Reference System (CRS)
// that determines the meaning of the values in the Coordinate.
sealed trait Coordinate {
  val name:String

  override def toString: String = name
}
// WGS-84: longitude, latitude (x,y)
object WGS_84 extends Coordinate {override val name: String = "WGS-84"}
// WGS-84: longitude, latitude, height (x,y,z)
object WGS_84_3D extends Coordinate {override val name: String = "WGS-84-3D"}
// Cartesian: x,y
object Cartesian  extends Coordinate {override val name: String = "Cartesian"}
// Cartesian: x,y,z
object Cartesian_3D extends Coordinate {override val name: String = "Cartesian_3D"}