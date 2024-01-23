package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.{DataFrame, InnerJoin, JoinType}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxBoolean, LynxNull}
import org.opencypher.v9_0.expressions.Expression

/*
 @param joinType: InnerJoinPPTApply/FullJoin/LeftJoin/RightJoin
 */
case class Join(filterExpr: Option[Expression],
                isSingleMatch: Boolean,
                joinType: JoinType)
               (l: PhysicalPlan, r: PhysicalPlan, val plannerContext: PhysicalPlannerContext)
  extends DoublePhysicalPlan(l,r) {
//  override val children: Seq[PhysicalPlan] = Seq(a, b)

  val a:PhysicalPlan = this.left.get
  val b:PhysicalPlan = this.right.get

  override def schema: Seq[(String, LynxType)] = (a.schema ++ b.schema).distinct

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df1 = a.execute(ctx)
    val df2 = b.execute(ctx)

    val df = df1.join(df2, isSingleMatch, joinType)

    if (filterExpr.nonEmpty) {
      val ec = ctx.expressionContext
      val ifNull = joinType match {
        case InnerJoin => false
        case _ => true
      }
      df.filter {
        (record: Seq[LynxValue]) =>
          eval(filterExpr.get)(ec.withVars(df.schema.map(_._1).zip(record).toMap)) match {
            case LynxBoolean(b) => b
            case LynxNull => ifNull
          }
      }(ec)
    }
    else df
  }

//  override def withChildren(children0: Seq[PhysicalPlan]): PPTJoin = PPTJoin(filterExpr, isSingleMatch, joinType)(children0.head, children0(1), plannerContext)


}
