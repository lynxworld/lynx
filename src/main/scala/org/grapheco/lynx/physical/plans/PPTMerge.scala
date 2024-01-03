package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical._
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.structural.LynxNode
import org.opencypher.v9_0.ast.{OnCreate, OnMatch}

import scala.collection.mutable

/**
 *
 * @param mergeSchema
 * @param mergeOps
 * @param onMatch
 * @param onCreate
 * @param in
 * @param plannerContext
 */
case class PPTMerge(mergeSchema: Seq[(String, LynxType)],
                    mergeOps: Seq[FormalElement],
                    onMatch: Seq[OnMatch],
                    onCreate: Seq[OnCreate])(l: Option[PhysicalPlan], val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalPlan(l) {

  override def schema: Seq[(String, LynxType)] = mergeSchema ++ left.map(_.schema).getOrElse(Seq.empty)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext
    // has match or create?
    var hasMatched = false
    // PPTMerge must has-only-has one child
    val child: PhysicalPlan = children.head
    // Set[(name, CreatedElement)]: aviod to create again
    val distinct: Boolean = !(mergeOps.collectFirst { case f: FormalNode => f }.nonEmpty && mergeOps.collectFirst { case f: FormalRelationship => f }.nonEmpty)

    val createdNode: mutable.HashMap[NodeInput, LynxNode] = if (distinct) mutable.HashMap[NodeInput, LynxNode]() else null

    val df = child match {
      case pj: PPTJoin => {
        val pjRes = pj.execute
        val dropNull = pjRes.records.dropWhile(_.exists(LynxNull.eq))
        if (dropNull.nonEmpty) {
          hasMatched = true
          pjRes.select(schema.map { case (name, _) => (name, None) })
        } else {
          val opsMap = mergeOps.map(ele => ele.varName -> ele).toMap
          val records = pjRes.select(schema.map { case (name, _) => (name, None) }).records.map { record =>
            val recordMap = schema.map(_._1).zip(record).toMap
            val nullCol = recordMap.filter(_._2 == LynxNull).keys.toSeq
            val creation = CreateOps(nullCol.map(opsMap))(e => eval(e)(ec.withVars(recordMap)), graphModel).execute(createdNode).toMap
            record.zip(schema.map(_._1)).map {
              case (LynxNull, name) => creation(name)
              case (v: LynxValue, _) => v
            }
          }.toList
          DataFrame(schema, () => records.toIterator) // danger! so do because the create ops will be do lazy due to inner in the dataframe which is lazy.
        }
      }
      case _ => {
        val res = child.execute.records
        if (res.nonEmpty) {
          hasMatched = true
          DataFrame(schema, () => res)
        } else {
          val creation = CreateOps(mergeOps)(e => eval(e)(ec), graphModel).execute()
          DataFrame(schema, () => Iterator(schema.map(x => creation.toMap.getOrElse(x._1, LynxNull))))
        }
      }
    }
    // actions
    val items = if (hasMatched) onMatch.flatMap(_.action.items) else onCreate.flatMap(_.action.items)
    if (items.nonEmpty) {
      PPTSet(items)(new PhysicalPlan { // temp PPTNode to execute SetClause
        override val schema: Seq[(String, LynxType)] = df.schema

        override def execute(implicit ctx: ExecutionContext): DataFrame = df

        override var left: Option[PhysicalPlan] = None
        override var right: Option[PhysicalPlan] = None
      }, plannerContext).execute(ctx)
    } else df
  }
}
