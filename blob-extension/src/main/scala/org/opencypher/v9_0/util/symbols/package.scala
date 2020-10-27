package org.opencypher.v9_0.util

package object symbols {
  //<--blob semantics
  val CTBlob: BlobType = BlobType.instance
  //blob semantics-->
  val CTAny: AnyType = AnyType.instance
  val CTBoolean: BooleanType = BooleanType.instance
  val CTString: StringType = StringType.instance
  val CTNumber: NumberType = NumberType.instance
  val CTFloat: FloatType = FloatType.instance
  val CTInteger: IntegerType = IntegerType.instance
  val CTMap: MapType = MapType.instance
  val CTNode: NodeType = NodeType.instance
  val CTRelationship: RelationshipType = RelationshipType.instance
  val CTPoint: PointType = PointType.instance
  val CTDateTime: DateTimeType = TemporalTypes.datetime
  val CTLocalDateTime: LocalDateTimeType = TemporalTypes.localdatetime
  val CTDate: DateType = TemporalTypes.date
  val CTTime: TimeType = TemporalTypes.time
  val CTLocalTime: LocalTimeType = TemporalTypes.localtime
  val CTDuration: DurationType = TemporalTypes.duration
  val CTGeometry: GeometryType = GeometryType.instance
  val CTPath: PathType = PathType.instance
  val CTGraphRef: GraphRefType = GraphRefType.instance
  def CTList(inner: CypherType): ListType = ListType(inner)

  implicit def invariantTypeSpec(that: CypherType): TypeSpec = that.invariant
}
