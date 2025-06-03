package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.structural.LynxNode
import org.grapheco.lynx.types.{LynxType, LynxValue}

import scala.collection.mutable.ArrayBuffer

/**
 * @Author renhao
 * @Description:
 * @Data 2025/2/25 15:26
 * @Modified By:
 */
case class Link()(l: PhysicalPlan, r: PhysicalPlan, val plannerContext: PhysicalPlannerContext)
  extends DoublePhysicalPlan(l,r) {

  override def schema: Seq[(String, LynxType)] = l.schema ++ r.schema.tail

  override def execute(implicit ctx: ExecutionContext): DataFrame = new DataFrame {
    val leftRecords = l.execute(ctx).records
    var rightRecords = r.execute(ctx).records

    override def schema: Seq[(String, LynxType)] = l.schema ++ r.schema.tail

    override def records: Iterator[Seq[LynxValue]] = new Iterator[Seq[LynxValue]] {
      var leftRecord: Option[Seq[LynxValue]] = if(leftRecords.hasNext) Some(leftRecords.next()) else None
      val rightRecordsSeq = new ArrayBuffer[Seq[LynxValue]]()
      var nextRecord: Option[Seq[LynxValue]] = getNextRecord()

      def getNextRecord(): Option[Seq[LynxValue]] = {
        while (leftRecord.nonEmpty){
          val leftNodeId = leftRecord.get.last.asInstanceOf[LynxNode].id
          while (rightRecords.hasNext){
            val rightRecord = rightRecords.next()
            rightRecordsSeq.append(rightRecord)
            val rightRecordId = rightRecord.head.asInstanceOf[LynxNode].id
            if(leftNodeId == rightRecordId) return Some(leftRecord.get ++ rightRecord.tail)
          }
          leftRecord = None
          if(leftRecords.hasNext) {
            leftRecord = Some(leftRecords.next())
            rightRecords = rightRecordsSeq.toList.toIterator
            rightRecordsSeq.clear()
          }
        }
        None
      }
      override def hasNext: Boolean = nextRecord.nonEmpty

      override def next(): Seq[LynxValue] = {
        val result = nextRecord.getOrElse(throw new NoSuchElementException("next on empty iterator"))
        nextRecord = getNextRecord()
        result
      }
    }
  }
}
