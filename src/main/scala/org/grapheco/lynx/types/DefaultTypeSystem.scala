package org.grapheco.lynx.types

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property._
import org.grapheco.lynx.types.spatial.{Cartesian2D, LynxPoint}
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxRelationship}
import org.grapheco.lynx.types.time._
import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTFloat, CTInteger, CTList, CTString, CypherType}

import java.time._
import scala.collection.mutable

/**
 * @ClassName DefaultTypeSystem
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/21
 * @Version 0.1
 */
class DefaultTypeSystem extends TypeSystem {
  val NULL_STRING = "(null)"
  val mapTypes: mutable.Map[Class[_], LynxType] = mutable.Map(
    //    classOf[BooleanLiteral] -> CTBoolean,
    //    classOf[StringLiteral] -> CTString,
    //    classOf[IntegerLiteral] -> CTInteger,
    //    classOf[DoubleLiteral] -> CTFloat,
    classOf[LynxBoolean] -> CTBoolean,
    classOf[LynxString] -> CTString,
    classOf[LynxInteger] -> CTInteger,
    classOf[LynxFloat] -> CTFloat,
    classOf[LynxList] -> CTList(CTAny),
    // todo: time and date.
  )

  override def typeOf(clazz: Class[_]): CypherType = mapTypes.getOrElse(clazz, CTAny)

  override def wrap(value: Any): LynxValue = value match {
    case null => LynxNull
    case v: LynxValue => v
    case v: Boolean => LynxBoolean(v)
    case v: Int => LynxInteger(v)
    case v: Long => LynxInteger(v)
    case v: String => LynxString(v)
    case v: Double => LynxFloat(v)
    case v: Float => LynxFloat(v)
    case v: LocalDate => LynxDate(v)
    case v: ZonedDateTime => LynxDateTime(v)
    case v: LocalDateTime => LynxLocalDateTime(v)
    case v: LocalTime => LynxLocalTime(v)
    case v: OffsetTime => LynxTime(v)
    case v: Duration => LynxDuration(v)
    case v: Map[String, Any] => LynxMap(v.mapValues(wrap))
    case v: Array[Int] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Long] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Double] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Float] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Boolean] => LynxList(v.map(wrap(_)).toList)
    case v: Array[String] => LynxList(v.map(wrap(_)).toList)
    case v: Array[Any] => LynxList(v.map(wrap(_)).toList)
    case v: Iterable[Any] => LynxList(v.map(wrap(_)).toList)
    case _ => throw InvalidValueException(value)
  }

  override def format(value: LynxValue): String = value match {//TODO time
    case LynxString(s) => "'" + s + "'"
    case LynxNull => NULL_STRING
    case LynxMap(m) => s"{${m.mapValues(format).mkString(", ")}}"
    case LynxList(l) => s"[${l.map(format).mkString(", ")}]"
    case n: LynxNode => s"${n.labels.map(":"+_.toString).mkString("")}" +
      s"{<id>: ${n.id}, ${n.keys.map{ key => key.toString+": "+ n.property(key).map(format).getOrElse(NULL_STRING)}.mkString(", ")}}"
    case r: LynxRelationship => s"${r.relationType.map(":"+_).getOrElse("")}{<id>: ${r.id}, <start>: ${r.startNodeId}, <end>: ${r.endNodeId}, " +
      s"${r.keys.map{ key => key.toString+": "+ r.property(key).map(format).getOrElse(NULL_STRING)}.mkString(", ")}}"
    case v => v.toString
  }
}
