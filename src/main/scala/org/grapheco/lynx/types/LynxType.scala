package org.grapheco.lynx.types

abstract class LynxType {
    def parentType: LynxType
    val isAbstract: Boolean = false
    def isAssignableFrom(other: LynxType): Boolean =
        if (other == this)
            true
        else if (other.parentType == other)
            false
        else
            isAssignableFrom(other.parentType)
}

object AnyType {
    val instance: AnyType = new AnyType() {
        val parentType: LynxType = this
        override val isAbstract = true

        override def isAssignableFrom(other: LynxType): Boolean = true

        override val toString = "Any"
    }
}

object BooleanType {
    val instance: BooleanType = new BooleanType() {
        val parentType: LynxType = LTAny
        override val toString = "Boolean"
    }
}

object StringType {
    val instance: StringType = new StringType() {
        val parentType: LynxType = LTAny
        override val toString = "String"
    }
}

object NumberType {
    val instance: NumberType = new NumberType() {
        val parentType: LynxType = LTAny
        override val toString = "Number"
    }
}

object FloatType {
    val instance: FloatType = new FloatType() {
        val parentType: LynxType = LTNumber
        override val toString = "Float"
    }
}

object IntegerType {
    val instance: IntegerType = new IntegerType() {
        val parentType: LynxType = LTNumber
        override val toString = "Integer"
    }
}

object MapType {
    val instance: MapType = new MapType() {
        val parentType: LynxType = LTAny
        override val toString = "Map"
    }
}
// TODO is parent = LTMap ?
object NodeType {
    val instance: NodeType = new NodeType() {
        val parentType: LynxType = LTAny
        override val toString = "Node"
    }
}

object RelationshipType {
    val instance: RelationshipType = new RelationshipType() {
        val parentType: LynxType = LTAny
        override val toString = "Relationship"
    }
}

object VirtualNodeType {
    val instance: VirtualNodeType = new VirtualNodeType() {
        val parentType: LynxType = LTAny
        override val toString = "VirtualNode"
    }
}

object VirtualRelationshipType {
    val instance: VirtualRelationshipType = new VirtualRelationshipType() {
        val parentType: LynxType = LTAny
        override val toString = "VirtualRelationship"
    }
}

object PointType {
    val instance: PointType = new PointType() {
        val parentType: LynxType = LTAny
        override val toString = "Point"
    }
}

object TemporalTypes {
    val datetime = new DateTimeType {
        val parentType = LTAny
        override val toString = "DateTime"
    }
    val localdatetime = new LocalDateTimeType {
        val parentType = LTAny
        override val toString = "LocalDateTime"
    }
    val date = new DateType {
        val parentType = LTAny
        override val toString = "Date"
    }
    val time = new TimeType {
        val parentType = LTAny
        override val toString = "Time"
    }
    val localtime = new LocalTimeType {
        val parentType = LTAny
        override val toString = "LocalTime"
    }
    val duration = new DurationType {
        val parentType = LTAny
        override val toString = "Duration"
    }
}

object GeometryType {
    val instance: GeometryType = new GeometryType() {
        val parentType: LynxType = LTAny
        override val toString = "Geometry"
    }
}

object PathType {
    val instance: PathType = new PathType() {
        val parentType: LynxType = LTAny
        override val toString = "Path"
    }
}

object GraphRefType {
    val instance: GraphRefType = new GraphRefType() {
        val parentType: LynxType = LTAny
        override val toString = "GraphRef"
    }
}

object ListType {
    private val anyCollectionTypeInstance = new ListTypeImpl(LTAny)

    def apply(iteratedType: LynxType) =
        if (iteratedType == LTAny) anyCollectionTypeInstance else new ListTypeImpl(iteratedType)

    final case class ListTypeImpl(innerType: LynxType) extends ListType {
        val parentType = LTAny
//        override val legacyIteratedType = innerType
//
//        override lazy val coercibleTo: Set[LynxType] = Set(CTBoolean) ++ parentType.coercibleTo

//        override def parents = innerType.parents.map(copy) ++ super.parents

        override val toString = s"List<$innerType>"

        override def isAssignableFrom(other: LynxType): Boolean = other match {
            case otherCollection: ListType =>
                innerType isAssignableFrom otherCollection.innerType
            case _ =>
                super.isAssignableFrom(other)
        }

//        override def leastUpperBound(other: LynxType) = other match {
//            case otherCollection: ListType =>
//                copy(innerType leastUpperBound otherCollection.innerType)
//            case _ =>
//                super.leastUpperBound(other)
//        }
//
//        override def greatestLowerBound(other: LynxType) = other match {
//            case otherCollection: ListType =>
//                (innerType greatestLowerBound otherCollection.innerType).map(copy)
//            case _ =>
//                super.greatestLowerBound(other)
//        }
//
//        override def rewrite(f: LynxType => LynxType) = f(copy(innerType.rewrite(f)))
    }

    def unapply(x: LynxType): Option[LynxType] = x match {
        case x: ListType => Some(x.innerType)
        case _ => None
    }
}


sealed abstract class AnyType extends LynxType
sealed abstract class BooleanType extends LynxType
sealed abstract class StringType extends LynxType
sealed abstract class NumberType extends LynxType
sealed abstract class FloatType extends LynxType
sealed abstract class IntegerType extends LynxType
sealed abstract class MapType extends LynxType
sealed abstract class NodeType extends LynxType
sealed abstract class VirtualNodeType extends LynxType
sealed abstract class RelationshipType extends LynxType
sealed abstract class VirtualRelationshipType extends LynxType
sealed abstract class PointType extends LynxType
sealed abstract class DateTimeType extends LynxType
sealed abstract class LocalDateTimeType extends LynxType
sealed abstract class DateType extends LynxType
sealed abstract class TimeType extends LynxType
sealed abstract class LocalTimeType extends LynxType
sealed abstract class DurationType extends LynxType
sealed abstract class GeometryType extends LynxType
sealed abstract class PathType extends LynxType
sealed abstract class GraphRefType extends LynxType
sealed abstract class ListType extends LynxType {
    def innerType: LynxType
}
