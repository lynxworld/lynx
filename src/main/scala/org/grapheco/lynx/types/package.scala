package org.grapheco.lynx

import org.opencypher.v9_0.util.symbols
import org.opencypher.v9_0.util.symbols.CypherType

import scala.language.implicitConversions

package object types {
  val LTAny: AnyType = AnyType.instance
  val LTBoolean: BooleanType = BooleanType.instance
  val LTString: StringType = StringType.instance
  val LTNumber: NumberType = NumberType.instance
  val LTFloat: FloatType = FloatType.instance
  val LTInteger: IntegerType = IntegerType.instance
  val LTMap: MapType = MapType.instance
  val LTNode: NodeType = NodeType.instance
  val LTVNode: VirtualNodeType = VirtualNodeType.instance
  val LTRelationship: RelationshipType = RelationshipType.instance
  val LTVRelationship: VirtualRelationshipType = VirtualRelationshipType.instance
  val LTPoint: PointType = PointType.instance
  val LTDateTime: DateTimeType = TemporalTypes.datetime
  val LTLocalDateTime: LocalDateTimeType = TemporalTypes.localdatetime
  val LTDate: DateType = TemporalTypes.date
  val LTTime: TimeType = TemporalTypes.time
  val LTLocalTime: LocalTimeType = TemporalTypes.localtime
  val LTDuration: DurationType = TemporalTypes.duration
  val LTGeometry: GeometryType = GeometryType.instance
  val LTPath: PathType = PathType.instance
  val LTGraphRef: GraphRefType = GraphRefType.instance
  def LTList(inner: LynxType): ListType = ListType(inner)

  implicit def CT2LT(ct: CypherType): LynxType = ct match {
    case booleanType: symbols.BooleanType => LTBoolean
    case integerType: symbols.IntegerType => LTInteger
    case durationType: symbols.DurationType => LTDuration
    case geometryType: symbols.GeometryType => LTGeometry
    case anyType: symbols.AnyType => LTAny
    case mapType: symbols.MapType => LTMap
    case dateType: symbols.DateType => LTDate
    case nodeType: symbols.NodeType => LTNode
    case relationshipType: symbols.RelationshipType => LTRelationship
    case vNodeType: symbols.VirtualNodeType => LTVNode
    case vRelationshipType: symbols.VirtualRelationshipType => LTVRelationship
    case pathType: symbols.PathType => LTPath
    case timeType: symbols.TimeType => LTTime
    case floatType: symbols.FloatType => LTFloat
    case pointType: symbols.PointType => LTPoint
    case numberType: symbols.NumberType => LTNumber
    case stringType: symbols.StringType => LTString
    case timeType: symbols.DateTimeType => LTDateTime
    case refType: symbols.GraphRefType => LTGraphRef
    case timeType: symbols.LocalTimeType => LTLocalDateTime
    case symbols.ListType(cypherType) => LTList(CT2LT(cypherType))
    case _ => LTAny
  }
}
