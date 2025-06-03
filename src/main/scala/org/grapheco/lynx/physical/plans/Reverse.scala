package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.{LynxType, LynxValue}

import scala.collection.mutable.ArrayBuffer

/**
 * @Author renhao
 * @Description:
 * @Data 2025/2/25 15:18
 * @Modified By:
 */
case class Reverse()(l: PhysicalPlan, val plannerContext: PhysicalPlannerContext)
  extends SinglePhysicalPlan(l) {

  override def schema: Seq[(String, LynxType)] = super.schema.reverse

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    new DataFrame {
      override def schema: Seq[(String, LynxType)] = reverseRelationship(df.schema)

      override def records: Iterator[Seq[LynxValue]] = df.records.map(reverseRelationship(_))
    }
  }

  private def reverseRelationship[T](l: Seq[T]): Seq[T] = {
    val pathLen = schema.length
    if(pathLen == l.length) l.reverse
    else{
      l.dropRight(pathLen) ++ l.takeRight(pathLen).reverse
    }
  }

}
