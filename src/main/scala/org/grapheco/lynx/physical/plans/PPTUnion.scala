package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.{PhysicalPlannerContext, SyntaxErrorException}
import org.grapheco.lynx.runner.ExecutionContext

////////////////////////////
case class PPTUnion(distinct: Boolean)(a: PhysicalPlan, b: PhysicalPlan, val plannerContext: PhysicalPlannerContext) extends AbstractPhysicalPlan {
  override val children: Seq[PhysicalPlan] = Seq(a, b)

  override val schema: Seq[(String, LynxType)] = a.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val schema1 = a.schema
    val schema2 = b.schema
    if (!schema1.toSet.equals(schema2.toSet)) throw SyntaxErrorException("All sub queries in an UNION must have the same column names")
    val record1 = a.execute(ctx).records
    val record2 = b.execute(ctx).records
    val df = DataFrame(schema, () => record1 ++ record2)
    if (distinct) df.distinct() else df
  }

  override def withChildren(children0: Seq[PhysicalPlan]): PhysicalPlan = PPTUnion(distinct)(children0.head, children0(1), plannerContext)
}
