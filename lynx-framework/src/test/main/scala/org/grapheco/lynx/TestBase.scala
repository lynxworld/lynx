package org.grapheco.lynx

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetTime, ZoneId, ZoneOffset, ZonedDateTime}

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.util.Profiler
import org.opencypher.v9_0.expressions.{LabelName, PropertyKeyName}
import org.opencypher.v9_0.util.symbols.{CTAny, CTDate, CTDateTime, CTInteger, CTLocalDateTime, CTLocalTime, CTString, CTTime}

import scala.collection.mutable.ArrayBuffer

class TestBase extends LazyLogging {
  Profiler.enableTiming = true

  //(bluejoe)-[:KNOWS]->(alex)-[:KNOWS]->(CNIC)
  //(bluejoe)-[]-(CNIC)
  val node1 = TestNode(1, Seq("person", "leader"), "gender" -> LynxValue("male"), "name" -> LynxValue("bluejoe"), "age" -> LynxValue(40))
  val node2 = TestNode(2, Seq("person"), "name" -> LynxValue("Alice"), "gender" -> LynxValue("female"), "age" -> LynxValue(30))
  val node3 = TestNode(3, Seq(), "name" -> LynxValue("Bob"), "gender" -> LynxValue("male"), "age" -> LynxValue(10))
  val all_nodes = ArrayBuffer[TestNode](node1, node2, node3)
  val all_rels = ArrayBuffer[TestRelationship](
    TestRelationship(1, 1, 2, Some("KNOWS")),
    TestRelationship(2, 2, 3, Some("KNOWS")),
    TestRelationship(3, 1, 3, None)
  )
  val allIndex = ArrayBuffer[(LabelName, List[PropertyKeyName])]()

  val NODE_SIZE = all_nodes.size
  val REL_SIZE = all_rels.size

  val model = new GraphModel {
    override def createElements[T](
      nodesInput: Seq[(String, NodeInput)],
      relsInput: Seq[(String, RelationshipInput)],
      onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T = {
      val nodesMap: Seq[(String, TestNode)] = nodesInput.map(x => {
        val (varname, input) = x
        val id = all_nodes.size + 1
        varname -> TestNode(id, input.labels, input.props: _*)
      })

      def nodeId(ref: NodeInputRef): Long = {
        ref match {
          case StoredNodeInputRef(id) => id.value.asInstanceOf[Long]
          case ContextualNodeInputRef(varname) => nodesMap.find(_._1 == varname).get._2.longId
        }
      }

      val relsMap: Seq[(String, TestRelationship)] = relsInput.map(x => {
        val (varname, input) = x
        varname -> TestRelationship(all_rels.size + 1, nodeId(input.startNodeRef), nodeId(input.endNodeRef), input.types.headOption)
      }
      )

      logger.debug(s"created nodes: ${nodesMap}, rels: ${relsMap}")
      all_nodes ++= nodesMap.map(_._2)
      all_rels ++= relsMap.map(_._2)

      onCreated(nodesMap, relsMap)
    }

    def nodeAt(id: Long): Option[LynxNode] = {
      all_nodes.find(_.longId == id)
    }

    override def nodes(): Iterator[LynxNode] = all_nodes.iterator

    override def relationships(): Iterator[PathTriple] =
      all_rels.map(rel =>
        PathTriple(nodeAt(rel.startId).get, rel, nodeAt(rel.endId).get)
      ).iterator

    override def createIndex(labelName: LabelName, properties: List[PropertyKeyName]): Unit = {
      allIndex += Tuple2(labelName, properties)
    }

    override def getIndexes(): Array[(LabelName, List[PropertyKeyName])] = {
      allIndex.toArray
    }

    override def filterNodesWithRelations(nodesIDs: Seq[LynxId]): Seq[LynxId] = {
      nodesIDs.filter(id => all_rels.filter(rel => rel.startNodeId==id || rel.endNodeId==id).nonEmpty)
    }

    override def deleteRelationsOfNodes(nodesIDs: Seq[LynxId]): Unit = {
      nodesIDs.foreach(id => all_rels --= all_rels.filter(rel => rel.startNodeId==id || rel.endNodeId==id))
    }

    override def deleteFreeNodes(nodesIDs: Seq[LynxId]): Unit = {
      nodesIDs.foreach(id => all_nodes --= all_nodes.filter(_.id==id))
    }


    override def setNodeProperty(nodeId: LynxId, data: Array[(String ,AnyRef)], withReturn: Boolean): Option[Seq[LynxValue]] = {
      val record = all_nodes.find(n => n.id == nodeId)
      if (record.isDefined){
        val node = record.get
        val property = scala.collection.mutable.Map(node.properties.toSeq:_*)
        data.foreach(f => property(f._1) = LynxValue(f._2))
        val newNode = TestNode(node.id.value.asInstanceOf[Long], node.labels, property.toSeq:_*)
        all_nodes -= node
        all_nodes += newNode
        if (withReturn) Option(Seq(newNode))
        else None
      }
      else None
    }

    override def addNodeLabels(nodeId: LynxId, labels: Array[String], withReturn: Boolean): Option[Seq[LynxValue]] = {
      val record = all_nodes.find(n => n.id == nodeId)
      if (record.isDefined){
        val node = record.get
        val newNode = TestNode(node.id.value.asInstanceOf[Long], (node.labels ++ labels).distinct, node.properties.toSeq:_*)
        all_nodes -= node
        all_nodes += newNode
        if (withReturn) Option(Seq(newNode))
        else None
      }
      else None
    }

    override def setRelationshipProperty(triple: Seq[LynxValue], data: Array[(String ,AnyRef)], withReturn: Boolean): Option[Seq[LynxValue]] = {
      val rel = triple(1).asInstanceOf[LynxRelationship]
      val record = all_rels.find(r => r.id == rel.id)
      if (record.isDefined){
        val relation = record.get
        val property = scala.collection.mutable.Map(relation.properties.toSeq:_*)
        data.foreach(f => property(f._1) = LynxValue(f._2))
        val newRelationship = TestRelationship(relation.id0, relation.startId, relation.endId, relation.relationType, property.toMap.toSeq:_*)
        all_rels -= relation
        all_rels += newRelationship
        if (withReturn) Option(Seq(triple.head, newRelationship, triple(2)))
        else None
      }
      else None
    }

    override def setRelationshipTypes(triple: Seq[LynxValue], labels: Array[String], withReturn: Boolean): Option[Seq[LynxValue]] = {
      val rel = triple(1).asInstanceOf[LynxRelationship]
      val record = all_rels.find(r => r.id == rel.id)
      if (record.isDefined){
        val relation = record.get
        val newRelationship = TestRelationship(relation.id0, relation.startId, relation.endId, Option(labels.head), relation.properties.toSeq:_*)
        all_rels -= relation
        all_rels += newRelationship
        if (withReturn) Option(Seq(triple.head, newRelationship, triple(2)))
        else None
      }
      else None
    }

    override def removeNodeProperty(nodeId: LynxId, data: Array[String], withReturn: Boolean): Option[Seq[LynxValue]] = {
      val record = all_nodes.find(n => n.id == nodeId)
      if (record.isDefined){
        val node = record.get
        val property = scala.collection.mutable.Map(node.properties.toSeq:_*)
        data.foreach(f => {if (property.contains(f)) property -= f} )
        val newNode = TestNode(node.id.value.asInstanceOf[Long], node.labels, property.toSeq:_*)
        all_nodes -= node
        all_nodes += newNode
        if (withReturn) Option(Seq(newNode))
        else None
      }
      else None
    }

    override def removeNodeLabels(nodeId: LynxId, labels: Array[String], withReturn: Boolean): Option[Seq[LynxValue]] = {
      val record = all_nodes.find(n => n.id == nodeId)
      if (record.isDefined){
        val node = record.get
        val newNode = TestNode(node.id.value.asInstanceOf[Long], (node.labels.toBuffer -- labels), node.properties.toSeq:_*)
        all_nodes -= node
        all_nodes += newNode
        if (withReturn) Option(Seq(newNode))
        else None
      }
      else None
    }

    override def removeRelationshipProperty(triple: Seq[LynxValue], data: Array[String], withReturn: Boolean): Option[Seq[LynxValue]] = {
      val rel = triple(1).asInstanceOf[LynxRelationship]
      val record = all_rels.find(r => r.id == rel.id)
      if (record.isDefined){
        val relation = record.get
        val property = scala.collection.mutable.Map(relation.properties.toSeq:_*)
        data.foreach(f => {if (property.contains(f)) property -= f} )
        val newRelationship = TestRelationship(relation.id0, relation.startId, relation.endId, relation.relationType, property.toMap.toSeq:_*)
        all_rels -= relation
        all_rels += newRelationship
        if (withReturn) Option(Seq(triple.head, newRelationship, triple(2)))
        else None
      }
      else None
    }

    override def removeRelationshipType(triple: Seq[LynxValue], labels: Array[String], withReturn: Boolean): Option[Seq[LynxValue]] = {
      val rel = triple(1).asInstanceOf[LynxRelationship]
      val record = all_rels.find(r => r.id == rel.id)
      if (record.isDefined){
        val relation = record.get
        val newType: Option[String] = {
          if (relation.relationType.get == labels.head) None
          else Option(relation.relationType.get)
        }
        val newRelationship = TestRelationship(relation.id0, relation.startId, relation.endId, newType, relation.properties.toSeq:_*)
        all_rels -= relation
        all_rels += newRelationship
        if (withReturn) Option(Seq(triple.head, newRelationship, triple(2)))
        else None
      }
      else None
    }
  }

  val runner = new CypherRunner(model) {
    val myfun = new DefaultProcedureRegistry(types, classOf[DefaultProcedures])
    override lazy val procedures: ProcedureRegistry = myfun

    myfun.register("test.authors", 0, new CallableProcedure {
      override val inputs: Seq[(String, LynxType)] = Seq()
      override val outputs: Seq[(String, LynxType)] = Seq("name" -> CTString)

      override def call(args: Seq[LynxValue]): LynxValue =
        LynxList(List(LynxValue("bluejoe"), LynxValue("lzx"), LynxValue("airzihao")))
    })

    myfun.register("toInterger", 1, new CallableProcedure {
      override val inputs: Seq[(String, LynxType)] = Seq("text" -> CTString)
      override val outputs: Seq[(String, LynxType)] = Seq("number" -> CTInteger)

      override def call(args: Seq[LynxValue]): LynxValue=
        LynxInteger(args.head.value.toString.toInt)
    })
  }

  protected def runOnDemoGraph(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult = {
    runner.compile(query)
    Profiler.timing {
      //call cache() only for test
      val rs = runner.run(query, param).cache()
      rs.show()
      rs
    }
  }
}

case class TestLynxId(value: Long) extends LynxId {
}

case class TestNode(longId: Long, labels: Seq[String], props: (String, LynxValue)*) extends LynxNode {
  lazy val properties = props.toMap
  override val id: LynxId = TestLynxId(longId)

  override def property(name: String): Option[LynxValue] = properties.get(name)
}

case class TestRelationship(id0: Long, startId: Long, endId: Long, relationType: Option[String], props: (String, LynxValue)*) extends LynxRelationship {
  lazy val properties = props.toMap
  override val id: LynxId = TestLynxId(id0)
  override val startNodeId: LynxId = TestLynxId(startId)
  override val endNodeId: LynxId = TestLynxId(endId)

  override def property(name: String): Option[LynxValue] = properties.get(name)
}

object LynxDateUtil {
  def parse(dateStr: String): LynxDate = {
    var v: LocalDate = null
    if (dateStr.contains('-')) {
      v = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    }
    else if (dateStr.contains('/')) {
      v = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy/MM/dd"))
    }
    else {
      v = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"))
    }

    LynxDate(v)
  }

  def of(year: Int, month: Int, day: Int): LynxDate = {
    LynxDate(LocalDate.of(year, month, day))
  }

  def ofEpochDay(epochDay: Long): LynxDate = {
    LynxDate(LocalDate.ofEpochDay(epochDay))
  }
}

object LynxDateTimeUtil {
  def parse(zonedDateTimeStr: String): LynxDateTime = {
    try{
      val v = ZonedDateTime.parse(zonedDateTimeStr)
      LynxDateTime(v)
    }catch  {
      case _ => throw new Exception("DateTimeParseException")
    }
  }

  def of(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int,
         nanosecond: Int, timezone: String): LynxDateTime = {
    val v = ZonedDateTime.of(year, month, day, hour, minute, second, nanosecond, parseZone(timezone))
    LynxDateTime(v)
  }

  def parseZone(zone: String): ZoneId = {
    if (zone == null || zone.isEmpty) {
      null
    }
    else if("Z".equalsIgnoreCase(zone)) {
      ZoneOffset.UTC
    }
    else if (zone.startsWith("+") || zone.startsWith("-")) {  // zone offset
      ZoneOffset.of(zone)
    }
    else {  // zone id
      ZoneId.of(zone)
    }
  }


}

object LynxLocalDateTimeUtil {
  def parse(localDateTimeStr: String): LynxLocalDateTime = {
    val v = LocalDateTime.parse(localDateTimeStr)
    LynxLocalDateTime(v)
  }

  def of(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int,
         nanosecond: Int): LynxLocalDateTime = {
    val v = LocalDateTime.of(year, month, day, hour, minute, second, nanosecond)
    LynxLocalDateTime(v)
  }
}

object LynxLocalTimeUtil {
  def parse(localTimeStr: String): LynxLocalTime = {
    val v = LocalTime.parse(localTimeStr)
    LynxLocalTime(v)
  }

  def of(hour: Int, minute: Int, second: Int, nanosOfSecond: Int): LynxLocalTime = {
    val v = LocalTime.of(hour, minute, second, nanosOfSecond)
    LynxLocalTime(v)
  }
}

object LynxTimeUtil {
  def parse(timeStr: String): LynxTime = {
    val v = OffsetTime.parse(timeStr)
    LynxTime(v)
  }

  def of (hour: Int, minute: Int, second: Int, nanosOfSecond: Int, offset: ZoneOffset): LynxTime = {
    val v = OffsetTime.of(hour, minute, second, nanosOfSecond, offset)
    LynxTime(v)
  }
}