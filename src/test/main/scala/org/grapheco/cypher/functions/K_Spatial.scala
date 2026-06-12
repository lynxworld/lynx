package org.grapheco.cypher.functions

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxFloat, LynxNull, LynxString}
import org.grapheco.lynx.types.structural._
import org.junit.jupiter.api.{Assertions, BeforeEach, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: Wangkainan
 * @create: 2022-09-02 15:50
 */


class K_Spatial extends TestBase {
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1),Seq(LynxNodeLabel("TrainStation")),
    Map(LynxPropertyKey("longitude") -> LynxValue(12.56459),
      LynxPropertyKey("city") -> LynxValue("Copenhagen"),
      LynxPropertyKey("latitude") -> LynxValue(55.672874)))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("Office")),
    Map(LynxPropertyKey("longitude") -> LynxValue(12.994341),
      LynxPropertyKey("city") -> LynxValue("Malmo"),
      LynxPropertyKey("latitude") -> LynxValue(55.611784)))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(2), Option(LynxRelationshipType("TRAVEL_ROUTE")), Map.empty)


  @BeforeEach
  def init(): Unit = {
    nodesInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodesInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))

    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))

    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
  }

  @Test
  def distance_1(): Unit = {
    val records = runOnDemoGraph(
      """
        |WITH point({ x: 2.3, y: 4.5, crs: 'cartesian' }) AS p1, point({ x: 1.1, y: 5.4, crs: 'cartesian' }) AS p2
        |RETURN distance(p1,p2) AS dist
        |""".stripMargin).records().toArray

    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals(LynxFloat(1.5), records(0)("dist"))
  }

  @Test
  def pointDistance_1(): Unit = {
    val records = runOnDemoGraph(
      """
        |WITH point({ x: 2.3, y: 4.5, crs: 'cartesian' }) AS p1, point({ x: 1.1, y: 5.4, crs: 'cartesian' }) AS p2
        |RETURN point.distance(p1,p2) AS dist
        |""".stripMargin).records().toArray

    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals(LynxFloat(1.5), records(0)("dist"))
  }

  @Test
  def distance_2(): Unit = {
    val records = runOnDemoGraph(
      """
        |WITH point({ longitude: 12.78, latitude: 56.7, height: 100 }) AS p1, point({ latitude: 56.71, longitude: 12.79, height: 100 }) AS p2
        |RETURN distance(p1,p2) AS dist
        |""".stripMargin).records().toArray

    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals(1269.9148706779097, records(0).getAsDouble("dist").get.v, 0.00001)
  }

  @Test
  def distance_3(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (t:TrainStation)-[:TRAVEL_ROUTE]->(o:Office)
        |WITH point({ longitude: t.longitude, latitude: t.latitude }) AS trainPoint, point({ longitude: o.longitude, latitude: o.latitude }) AS officePoint
        |RETURN round(distance(trainPoint, officePoint)) AS travelDistance
        |""".stripMargin).records().toArray

    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals(LynxFloat(27842.0), records(0)("travelDistance"))
  }

  @Test
  def distance_4(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN distance(NULL , point({ longitude: 56.7, latitude: 12.78 })) AS d
        |""".stripMargin).records().toArray

    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals(LynxNull, records(0)("d"))
  }

  @Test
  def point_WGS_84_2D_1(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN point({ longitude: 56.7, latitude: 12.78 }) AS point
        |""".stripMargin).records().toArray

    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals("point({x: 56.7, y: 12.78, crs: 'wgs-84'})", records(0)("point").asInstanceOf[LynxValue].value.toString  )
  }

  @Test
  def point_WGS_84_2D_2(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN point({ x: 2.3, y: 4.5, crs: 'WGS-84' }) AS point
        |""".stripMargin).records().toArray

    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals("point({x: 2.3, y: 4.5, crs: 'wgs-84'})", records(0)("point").asInstanceOf[LynxValue].value.toString)
  }

  @Test
  def point_WGS_84_2D_3(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (p:Office)
        |RETURN point({ longitude: p.longitude, latitude: p.latitude }) AS officePoint
        |""".stripMargin).records().toArray

    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals("point({x: 12.994341, y: 55.611784, crs: 'wgs-84'})", records(0)("officePoint").asInstanceOf[LynxValue].value.toString)
  }

  @Test
  def point_WGS_84_2D_4(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN point(NULL ) AS p
        |""".stripMargin).records().toArray

    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals(LynxNull, records(0)("p"))
  }

  @Test
  def point_WGS_84_3D(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN point({ longitude: 56.7, latitude: 12.78, height: 8 }) AS point
        |""".stripMargin).records().toArray

    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals("point({x: 56.7, y: 12.78, z: 8.0, crs: 'wgs-84-3d'})", records(0)("point").asInstanceOf[LynxValue].value.toString)
  }


  @Test
  def point_Cartesian_2D(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN point({ x: 2.3, y: 4.5 }) AS point
        |""".stripMargin).records().toArray

    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals("point({x: 2.3, y: 4.5, crs: 'cartesian'})", records(0)("point").asInstanceOf[LynxValue].value.toString)
  }

  @Test
  def point_Cartesian_3D(): Unit = {
    val records = runOnDemoGraph(
      """
        |RETURN point({ x: 2.3, y: 4.5, z: 2 }) AS point
        |""".stripMargin).records().toArray

    Assertions.assertEquals(1, records.length)
    Assertions.assertEquals("point({x: 2.3, y: 4.5, z: 2.0, crs: 'cartesian-3d'})", records(0)("point").asInstanceOf[LynxValue].value.toString)
  }
}

