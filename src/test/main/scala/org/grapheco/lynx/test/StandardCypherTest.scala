package org.grapheco.lynx.test

import org.grapheco.lynx._
import org.junit.jupiter.api.{DynamicTest, TestFactory}
import org.opencypher.tools.tck.api.{CypherTCK, CypherValueRecords, Graph, QueryType, SideEffectQuery}
import org.opencypher.tools.tck.values._

import scala.collection.JavaConverters._
/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-03-02 10:09
 */
class StandardCypherTest{

  @TestFactory
  def testStandardTck(): java.util.Collection[DynamicTest] ={
    val tckScenarios = CypherTCK.allTckScenarios

    def emptyGraph(): Graph = new MyGraph

    val dynamicTests = tckScenarios.map{
      scenario =>
        val name = scenario.name
        val executable = scenario(emptyGraph)
        DynamicTest.dynamicTest(name, executable)
    }
    dynamicTests.asJavaCollection
  }
}

class MyGraph() extends Graph{
  val db = new TestBase
  override def execute(query: String, params: Map[String, CypherValue], meta: QueryType): (Graph, Result) = {
    val res = db.runner.run(query, params.map(kv => (kv._1, cypherValue2ScalaValue(kv._2))))
    val result = resultFromValueRecords(CypherValueRecords(res.columns().toList, res.records().toList.map(m => m.map(kv => (kv._1, lynxValue2CypherValue(kv._2))))))
    (this, result)
  }

  def lynxValue2CypherValue(value: Any): CypherValue ={
    value match {
      case node: db.TestNode => {
        val labels = node.labels.map(l => l.value).toSet
        val props = CypherPropertyMap(node.props.map(kv => (kv._1.value, lynxValue2CypherValue(kv._2))))
        CypherNode(labels, props)
      }
      case relation: db.TestRelationship => {
        val rType = relation.relationType.get.value
        val props = CypherPropertyMap(relation.props.map(kv => (kv._1.value, lynxValue2CypherValue(kv._2))))
        CypherRelationship(rType, props)
      }
      case LynxBoolean(v) => CypherBoolean(v)
      case LynxDouble(v) => CypherFloat(v)
      case LynxDuration(duration) => CypherString(duration.toString)
      case LynxInteger(v) => CypherInteger(v)
      case LynxList(v) => CypherOrderedList(v.map(lynxValue2CypherValue))
      case LynxMap(v) => CypherPropertyMap(v.map(kv => (kv._1, lynxValue2CypherValue(kv._2))))
      case LynxString(v) => CypherString(v)
      case LynxDateTime(zonedDateTime) => CypherString(zonedDateTime.toString)
      case LynxDate(localDate) => CypherString(localDate.toString)
      case LynxLocalDateTime(localDateTime) => CypherString(localDateTime.toString)
      case LynxLocalTime(localTime) => CypherString(localTime.toString)
      case LynxTime(offsetTime) => CypherString(offsetTime.toString)
    }
  }

  def cypherValue2ScalaValue(cypherValue: CypherValue): Any ={
    cypherValue match {
      case CypherBoolean(value) => value
      case CypherFloat(value) => value
      case CypherInteger(value) => value
      case CypherOrderedList(values) => values.map(cypherValue2ScalaValue)
      case CypherNode(labels, properties) => ???
      case CypherRelationship(relType, properties) => ???
      case CypherNull => null
      case CypherString(s) => s
      case CypherPropertyMap(properties) => properties.map(kv => (kv._1, cypherValue2ScalaValue(kv._2)))
    }
  }
}

