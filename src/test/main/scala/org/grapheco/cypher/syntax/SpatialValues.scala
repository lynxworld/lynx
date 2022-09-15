package org.grapheco.cypher.syntax

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.types.LynxValue
import org.junit.{Assert, Test}

class SpatialValues extends TestBase{
  @Test
  def GeoCoordinateRefSys():Unit={
    val records = runOnDemoGraph("WITH point({ latitude:toFloat('13.43'), longitude:toFloat('56.21')}) AS p1, point({ latitude:toFloat('13.10'), longitude:toFloat('56.41')}) AS p2\nRETURN toInteger(distance(p1,p2)/1000) AS km")
      .records().map(f=>f("km").value).toArray
    Assert.assertEquals(42,records(0))
  }

  @Test
  def CartesianCoordinateRefSys():Unit={
    val records = runOnDemoGraph("WITH point({ x:3, y:0 }) AS p2d, point({ x:0, y:4, z:1 }) AS p3d\nRETURN distance(p2d,p3d) AS bad, distance(p2d,point({ x:p3d.x, y:p3d.y })) AS good")
      .records().map(f=>Map("bad"->f("bad"), "good"->f("good"))).toArray

    val expectResult = Map("bad"->null, "good"->LynxValue(5.0))

    expectResult.foreach(f=>{
      Assert.assertEquals(f._2,records(0)(f._1))
    })
  }

  @Test
  def createPoints():Unit={
    val records = runOnDemoGraph("RETURN point({ x:3, y:0 }) AS cartesian_2d, point({ x:0, y:4, z:1 }) AS cartesian_3d, point({ latitude: 12, longitude: 56 }) AS geo_2d, point({ latitude: 12, longitude: 56, height: 1000 }) AS geo_3d")
      .records().map(f=>Map(
      "cartesian_2d"->f("cartesian_2d"),
      "cartesian_3d"->f("cartesian_2d"),
      "geo_2d"->f("geo_2d"),
      "geo_3d"->f("geo_3d")
    ))
    /*Don't know how to create point object, so use this instead*/
    Assert.assertEquals(1,records.length)
  }

  @Test
  def accessPoint2D():Unit={
    val records = runOnDemoGraph("WITH point({ x:3, y:4 }) AS p\nRETURN p.x, p.y, p.crs, p.srid").records().map(f=>Map(
      "p.x"->f("p.x"),
      "p.y"->f("p.y"),
      "p.crs"->f("p.crs"),
      "p.srid"->f("p.srid")
    )).toArray

    val expectResult = Map(
      "p.x"->LynxValue(3.0),
      "p.y"->LynxValue(4.0),
      "p.crs"->LynxValue("cartesian"),
      "p.srid"->LynxValue("7203")
    )

    expectResult.foreach(f=>{
      Assert.assertEquals(f._2,records(0)(f._1))
    })
  }

  @Test
  def accessPoint3D():Unit={
    val records = runOnDemoGraph("WITH point({ latitude:3, longitude:4, height: 4321 }) AS p\nRETURN p.latitude, p.longitude, p.height, p.x, p.y, p.z, p.crs, p.srid").records().map(f=>Map(
      "p.latitude"->f("p.latitude"),
      "p.longitude"->f("p.longitude"),
      "p.height"->f("p.height"),
      "p.x"->f("p.x"),
      "p.y"->f("p.y"),
      "p.z"->f("p.z"),
      "p.crs"->f("p.crs"),
      "p.srid"->f("p.srid")
    )).toArray

    val expectResult = Map(
      "p.latitude"->LynxValue(3.0),
      "p.longitude"->LynxValue(4.0),
      "p.height"->LynxValue(4321.0),
      "p.x"->LynxValue(4.0),
      "p.y"->LynxValue(3.0),
      "p.z"->LynxValue(4321.0),
      "p.crs"->LynxValue("wgs-84-3d"),
      "p.srid"->LynxValue(4979)
    )

    expectResult.foreach(f=>{
      Assert.assertEquals(f._2,records(0)(f._1))
    })
  }

  @Test
  def testPointComparability():Unit={
    val records = runOnDemoGraph("WITH point({ x:3, y:0 }) AS p2d, point({ x:0, y:4, z:1 }) AS p3d\nRETURN distance(p2d,p3d), p2d < p3d, p2d = p3d, p2d <> p3d, distance(p2d,point({ x:p3d.x, y:p3d.y }))").records().map(f=>Map(
      "distance(p2d,p3d)"->f("distance(p2d,p3d)"),
      "p2d < p3d"->f("p2d < p3d"),
      "p2d = p3d"->f("p2d = p3d"),
      "p2d <> p3d"->f("p2d <> p3d"),
      "distance(p2d,point({ x:p3d.x, y:p3d.y }))"->f("distance(p2d,point({ x:p3d.x, y:p3d.y }))")
    )).toArray

    val expectResult = Map(
      "distance(p2d,p3d)"->null,
      "p2d < p3d"->null,
      "p2d = p3d"->LynxValue(false),
      "p2d <> p3d"->LynxValue(true),
      "distance(p2d,point({ x:p3d.x, y:p3d.y }))"->LynxValue(5.0)
    )

    expectResult.foreach(f=>{
      Assert.assertEquals(f._2,records(0)(f._1))
    })
  }

  /**
   * this function will test point for ordering, but point class is not be defined.
   */
  //  @Test
//  def testPointOrderability():Unit={
//    val records = runOnDemoGraph("UNWIND [point({ x:3, y:0 }), point({ x:0, y:4, z:1 }), point({ srid:4326, x:12, y:56 }), point({ srid:4979, x:12, y:56, z:1000 })] AS point\nRETURN point\nORDER BY point").records().map(f=>f("point").asInstanceOf[])
//  }
}
