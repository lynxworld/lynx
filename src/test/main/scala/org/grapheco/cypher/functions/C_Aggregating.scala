package org.grapheco.cypher.functions

import org.grapheco.lynx.TestBase
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural._
import org.junit.{Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description:
 * @author: Wangkainan
 * @create: 2022-08-30 14:25
 */
class C_Aggregating extends TestBase {
  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationsInput = ArrayBuffer[(String, RelationshipInput)]()

  val n1 = TestNode(TestId(1), Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("A"),
      LynxPropertyKey("age") -> LynxValue("13")))
  val n2 = TestNode(TestId(2), Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("B"),
      LynxPropertyKey("age") -> LynxValue("33"),
      LynxPropertyKey("eyes") -> LynxValue("blue")))
  val n3 = TestNode(TestId(3), Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("C"),
      LynxPropertyKey("age") -> LynxValue("44"),
      LynxPropertyKey("eyes") -> LynxValue("blue")))
  val n4 = TestNode(TestId(4), Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("D"),
      LynxPropertyKey("eyes") -> LynxValue("brown")))
  val n5 = TestNode(TestId(5), Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("D")))

  val r1 = TestRelationship(TestId(1), TestId(1), TestId(2), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r2 = TestRelationship(TestId(2), TestId(1), TestId(3), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r3 = TestRelationship(TestId(3), TestId(1), TestId(4), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r4 = TestRelationship(TestId(4), TestId(2), TestId(5), Option(LynxRelationshipType("KNOWS")), Map.empty)
  val r5 = TestRelationship(TestId(5), TestId(3), TestId(5), Option(LynxRelationshipType("KNOWS")), Map.empty)

  @Before
  def init(): Unit = {
    nodesInput.append(("n1", NodeInput(n1.labels, n1.props.toSeq)))
    nodesInput.append(("n2", NodeInput(n2.labels, n2.props.toSeq)))
    nodesInput.append(("n3", NodeInput(n3.labels, n3.props.toSeq)))
    nodesInput.append(("n4", NodeInput(n4.labels, n4.props.toSeq)))
    nodesInput.append(("n5", NodeInput(n5.labels, n5.props.toSeq)))

    relationsInput.append(("r1", RelationshipInput(Seq(r1.relationType.get), Seq.empty, StoredNodeInputRef(r1.startNodeId), StoredNodeInputRef(r1.endNodeId))))
    relationsInput.append(("r2", RelationshipInput(Seq(r2.relationType.get), Seq.empty, StoredNodeInputRef(r2.startNodeId), StoredNodeInputRef(r2.endNodeId))))
    relationsInput.append(("r3", RelationshipInput(Seq(r3.relationType.get), Seq.empty, StoredNodeInputRef(r3.startNodeId), StoredNodeInputRef(r3.endNodeId))))
    relationsInput.append(("r4", RelationshipInput(Seq(r4.relationType.get), Seq.empty, StoredNodeInputRef(r4.startNodeId), StoredNodeInputRef(r4.endNodeId))))
    relationsInput.append(("r5", RelationshipInput(Seq(r5.relationType.get), Seq.empty, StoredNodeInputRef(r5.startNodeId), StoredNodeInputRef(r5.endNodeId))))

    model.write.createElements(nodesInput, relationsInput,
      (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
        nodesCreated.toMap ++ relsCreated
      }
    )
  }


  @Test
  def avgNumericValues(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |RETURN avg(n.age)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(30.0, records(0)("avg(n.age)").asInstanceOf[LynxValue].value)
  }

  @Test
  def avgDurations(): Unit = {
    val records = runOnDemoGraph(
      """
        |UNWIND [duration('P2DT3H'), duration('PT1H45S')] AS dur
        |RETURN avg(dur)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("P1DT2H22.5S", records(0)("avg(dur)").asInstanceOf[LynxValue].value)
  }

  /*
   <null> as the value returned
   */
  @Test
  def collect(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |RETURN collect(n.age)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    val array_Expect = List(LynxString("13"), LynxString("44"), LynxString("33"))
    val array_Actual = records.head("collect(n.age)").asInstanceOf[LynxList].value.toList
    Assert.assertEquals(array_Expect.diff(array_Actual), array_Actual.diff(array_Expect))

    //    for (record <- records.head("collect(n.age)").asInstanceOf[LynxList].value.toList) {
    //      val index = record.value
    //      index match {
    //        case 13 => Assert.assertEquals("13", index)
    //        case 44 => Assert.assertEquals("44", index)
    //        case 33 => Assert.assertEquals("33", index)
    //        case _ => Assert.assertEquals(true, false)
    //      }
    //    }
  }

  @Test
  def usingCountToReturnTheNumberOfNodes(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n { name: 'A' })-->(x)
        |RETURN labels(n), n.age, count(*)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(List(LynxString("Person")), records(0)("labels(n)").asInstanceOf[LynxValue].value)
    Assert.assertEquals(13.toString, records(0)("n.age").asInstanceOf[LynxValue].value)
    Assert.assertEquals(3, records(0)("count(*)").asInstanceOf[LynxValue].value)
  }

  /*
  count(*) should be Integer
   */
  @Test
  def usingCountTotoGroupAndCountRelationshipTypes(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n { name: 'A' })-[r]->()
        |RETURN type(r), count(*)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("KNOWS", records(0)("type(r)").asInstanceOf[LynxValue].value)
    Assert.assertEquals(3, records(0)("count(*)").asInstanceOf[LynxValue].value)
  }


  /*
  return should be an Integer
   */
  @Test
  def usingCountToReturnTheNumberOfValues(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n { name: 'A' })-->(x)
        |RETURN count(x)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(3, records(0)("count(x)").asInstanceOf[LynxValue].value)
  }

  /*
    count all values include non-null values
   */
  @Test
  def countingNonNullValues(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |RETURN count(n.age)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(3.toLong, records(0)("count(n.age)").asInstanceOf[LynxValue].value)
  }

  @Test
  def countingWithAndWithoutDuplicates(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (me:Person)-->(friend:Person)-->(friend_of_friend:Person)
        |WHERE me.name = 'A'
        |RETURN count(DISTINCT friend_of_friend), count(friend_of_friend)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(1.toLong, records(0)("count(DISTINCT friend_of_friend)").asInstanceOf[LynxValue].value)
    Assert.assertEquals(2.toLong, records(0)("count(friend_of_friend)").asInstanceOf[LynxValue].value)
  }

  @Test
  def max_1(): Unit = {
    val records = runOnDemoGraph(
      """
        |UNWIND [1, 'a', NULL , 0.2, 'b', '1', '99'] AS val
        |RETURN max(val)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(1.toLong, records(0)("max(val)").asInstanceOf[LynxValue].value)
  }

  @Test
  def max_2(): Unit = {
    val records = runOnDemoGraph(
      """
        |UNWIND [[1, 'a', 89],[1, 2]] AS val
        |RETURN max(val)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(List(LynxInteger(1), LynxInteger(2)), records(0)("max(val)").asInstanceOf[LynxValue].value)
  }


  /*
   return should be an Integer
   */
  @Test
  def max_3(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |RETURN max(n.age)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(44, records(0)("max(n.age)").asInstanceOf[LynxValue].value)
  }

  @Test
  def min_1(): Unit = {
    val records = runOnDemoGraph(
      """
        |UNWIND [1, 'a', NULL , 0.2, 'b', '1', '99'] AS val
        |RETURN min(val)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("1", records(0)("min(val)").asInstanceOf[LynxValue].value)
  }

  @Test
  def min_2(): Unit = {
    val records = runOnDemoGraph(
      """
        |UNWIND ['d',[1, 2],['a', 'c', 23]] AS val
        |RETURN min(val)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(List(LynxString("a"), LynxString("c"), LynxInteger(23)), records(0)("min(val)").asInstanceOf[LynxValue].value)
  }

  /*
  no value returned
   */
  @Test
  def min_3(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |RETURN min(n.age)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(13, records(0)("min(n,age)").asInstanceOf[LynxValue].value)
  }

  @Test
  def percentileCont(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |RETURN percentileCont(n.age, 0.4)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(29.0, records(0)("percentileCont(n.age, 0.4)").asInstanceOf[LynxValue].value)
  }

  @Test
  def percentileDisc(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |RETURN percentileDisc(n.age, 0.5)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(33.toString, records(0)("percentileDisc(n.age, 0.5)").asInstanceOf[LynxValue].value)
  }

  @Test
  def stDev(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n)
        |WHERE n.name IN ['A', 'B', 'C']
        |RETURN stDev(n.age)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(15.716233645501712, records(0)("stDev(n.age)").asInstanceOf[LynxValue].value)
  }

  @Test
  def stDevP(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n)
        |WHERE n.name IN ['A', 'B', 'C']
        |RETURN stDevP(n.age)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(12.832251036613439, records(0)("stDevP(n.age)").asInstanceOf[LynxValue].value)
  }

  @Test
  def sumNumericValues(): Unit = {
    val records = runOnDemoGraph(
      """
        |MATCH (n:Person)
        |RETURN sum(n.age)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals(90, records(0)("sum(n.age)").asInstanceOf[LynxValue].value)
  }

  @Test
  def sumDurations(): Unit = {
    val records = runOnDemoGraph(
      """
        |UNWIND [duration('P2DT3H'), duration('PT1H45S')] AS dur
        |RETURN sum(dur)
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, records.length)
    Assert.assertEquals("P2DT4H45S", records(0)("sum(dur)").asInstanceOf[LynxValue].value)
  }
}
