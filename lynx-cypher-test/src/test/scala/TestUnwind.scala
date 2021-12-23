import org.grapheco.lynx.{LynxString, LynxValue}
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

/**
 * @program: lynx
 * @description: not impl
 * @author: LiamGao
 * @create: 2021-07-05 13:36
 */
class TestUnwind {
  val n1 = TestNode(1, Seq.empty, ("name",LynxString("Anders")))
  val n2 = TestNode(2, Seq.empty, ("name",LynxString("Caesar")))
  val n3 = TestNode(3, Seq.empty, ("name",LynxString("Bossman")))
  val n4 = TestNode(4, Seq.empty, ("name",LynxString("George")))
  val n5 = TestNode(5, Seq.empty, ("name",LynxString("David")))


  val r1 = TestRelationship(1, 1, 2, Option("BLOCKS"))
  val r2 = TestRelationship(2, 1, 3, Option("KNOWS"))
  val r3 = TestRelationship(3, 2, 4, Option("KNOWS"))
  val r4 = TestRelationship(4, 3, 4, Option("KNOWS"))
  val r5 = TestRelationship(5, 3, 5, Option("BLOCKS"))
  val r6 = TestRelationship(6, 5, 1, Option("KNOWS"))


  val nodesBuffer = new ArrayBuffer[TestNode]()
  val relsBuffer = new ArrayBuffer[TestRelationship]()
  nodesBuffer.append(n1, n2, n3, n4, n5)
  relsBuffer.append(r1, r2, r3, r4, r5, r6)

  val testBase = new TestBase(nodesBuffer, relsBuffer)

  @Test
  def unwindAList(): Unit = {
    val res = testBase.runOnDemoGraph(
      """
        |UNWIND [1, 2, 3, null] AS x
        |RETURN x, 'val' AS y
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res(0)("x").asInstanceOf[LynxValue].value)
    Assert.assertEquals(2, res(1)("x").asInstanceOf[LynxValue].value)
    Assert.assertEquals(3, res(2)("x").asInstanceOf[LynxValue].value)
    Assert.assertEquals(null, res(3)("x").asInstanceOf[LynxValue].value)

    Assert.assertEquals("val", res(0)("y").asInstanceOf[LynxValue].value)
    Assert.assertEquals("val", res(1)("y").asInstanceOf[LynxValue].value)
    Assert.assertEquals("val", res(2)("y").asInstanceOf[LynxValue].value)
    Assert.assertEquals("val", res(3)("y").asInstanceOf[LynxValue].value)
  }

  @Test
  def creatingADistinctList(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |WITH [1, 1, 2, 2] AS coll
        |UNWIND coll AS x
        |WITH DISTINCT x
        |RETURN collect(x) AS setOfVals
        |""".stripMargin).records().toArray
    Assert.assertEquals(List(1, 2).map(LynxValue(_)), res(0)("setOfVals").asInstanceOf[LynxValue].value)
  }

  @Test
  def usingUNWINDWithAnyExpressionReturningAList(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |WITH [1, 2] AS a,[3, 4] AS b
        |UNWIND (a + b) AS x
        |RETURN x
        |""".stripMargin).records().toArray

    Assert.assertEquals(1, res(0)("x").asInstanceOf[LynxValue].value)
    Assert.assertEquals(2, res(1)("x").asInstanceOf[LynxValue].value)
    Assert.assertEquals(3, res(2)("x").asInstanceOf[LynxValue].value)
    Assert.assertEquals(4, res(3)("x").asInstanceOf[LynxValue].value)
  }

  @Test
  def usingUNWINDWithAListOfLists(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |WITH [[1, 2],[3, 4], 5] AS nested
        |UNWIND nested AS x
        |UNWIND x AS y
        |RETURN y
        |""".stripMargin).records().toArray
    Assert.assertEquals(1, res(0)("y").asInstanceOf[LynxValue].value)
    Assert.assertEquals(2, res(1)("y").asInstanceOf[LynxValue].value)
    Assert.assertEquals(3, res(2)("y").asInstanceOf[LynxValue].value)
    Assert.assertEquals(4, res(3)("y").asInstanceOf[LynxValue].value)
    Assert.assertEquals(5, res(4)("y").asInstanceOf[LynxValue].value)
  }

  @Test
  def usingUNWINDWithEmptyList(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |UNWIND [] AS empty
        |RETURN empty, 'literal_that_is_not_returned'
        |""".stripMargin).records().toArray
    Assert.assertEquals(0, res.length)
  }

  @Test
  def usingUNWINDWithAnExpressionThatIsNotAList(): Unit ={
    val res = testBase.runOnDemoGraph(
      """
        |UNWIND [] AS empty
        |RETURN empty, 'literal_that_is_not_returned'
        |""".stripMargin).records().toArray
    Assert.assertEquals(0, res.length)
  }
}
