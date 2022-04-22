package org.grapheco.lynx

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxString}
import org.grapheco.lynx.types.time.{LynxDate, LynxDateTime, LynxDuration, LynxLocalDateTime, LynxLocalTime, LynxTime}
import org.junit.jupiter.api.{DynamicTest, TestFactory}
import org.opencypher.tools.tck.api.{CypherTCK, CypherValueRecords, Graph, QueryType}
import org.opencypher.tools.tck.values._

import scala.collection.JavaConverters._
import scala.language.{implicitConversions, postfixOps}
/**
 * @program: lynx
 * @description:
 * @author: LiamGao
 * @create: 2022-03-02 10:09
 */
class StandardCypherTest extends LazyLogging{

  val skip_duration = 195

  @TestFactory
  def testStandardTck(): java.util.Collection[DynamicTest] ={
    val allTckScenarios = CypherTCK.allTckScenarios

    def emptyGraph: Graph = new TestGraph

    val dynamicTests = allTckScenarios.slice(skip_duration, skip_duration + 100) map{
      scenario =>
        val name = scenario.name
        val executable = scenario(emptyGraph)
        DynamicTest.dynamicTest(name, executable)
    }
    dynamicTests.asJavaCollection
  }
}

class TestGraph extends TestBase with Graph {

  override def execute(query: String, params: Map[String, CypherValue], meta: QueryType): (Graph, Result) = {
    println(query, params)
    val scalaParams = params.mapValues(cypherValue2ScalaValue)

    val lynxResult = this.runner.run(query, scalaParams)

    (this, CypherValueRecords(
      lynxResult.columns().toList,
      lynxResult.records().map(_.mapValues(lynxValue2CypherValue)).toList
    ))
  }

  implicit def lynxValue2CypherValue(value: Any): CypherValue ={
    value match {
      case node: TestNode => CypherNode(
        node.labels.map(_.value).toSet,
        CypherPropertyMap(node.props.map{ case(propName, lynxValue) => (propName.value, lynxValue2CypherValue(lynxValue))})
      )
      case relation: TestRelationship => CypherRelationship(
        relation.relationType.map(_.value).getOrElse(""),
        CypherPropertyMap(relation.props.map{ case(propName, lynxValue) => (propName.value, lynxValue2CypherValue(lynxValue))})
      )
      case LynxBoolean(v) => CypherBoolean(v)
      case LynxFloat(v) => CypherFloat(v)
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
//      case CypherPropertyMap(properties) => properties.map(kv => (kv._1, cypherValue2ScalaValue(kv._2)))
      case CypherPropertyMap(properties) => properties mapValues cypherValue2ScalaValue
    }
  }
}

