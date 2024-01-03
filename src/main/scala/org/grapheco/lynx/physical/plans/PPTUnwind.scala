package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.opencypher.v9_0.expressions.{Expression, Variable}
import org.opencypher.v9_0.util.symbols.CTAny

case class PPTUnwind(expression: Expression, variable: Variable)(l: Option[PhysicalPlan], val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalPlan(l) {
  def in: Option[PhysicalPlan] = this.left
  override def schema: Seq[(String, LynxType)] = in.map(_.schema).getOrElse(Seq.empty) ++ Seq((variable.name, CTAny)) // TODO it is CTAny?

  override def execute(implicit ctx: ExecutionContext): DataFrame = // fixme
    in map { inNode =>
      val df = inNode.execute(ctx) // dataframe of in
      val colName = schema map { case (name, _) => name }
      DataFrame(schema, () => df.records flatMap { record =>
        val recordCtx = ctx.expressionContext.withVars(colName.zip(record).toMap)
        val rsl = (expressionEvaluator.eval(expression)(recordCtx) match {
          case list: LynxList => list.value
          case element: LynxValue => List(element)
        }) map { element => record :+ element }
        rsl
      })
    } getOrElse {
      DataFrame(schema, () =>
        eval(expression)(ctx.expressionContext).asInstanceOf[LynxList].value.toIterator map (lv => Seq(lv)))
    }

  override def withChildren(children0: Seq[PhysicalPlan]): PPTUnwind = PPTUnwind(expression, variable)(children0.headOption, plannerContext)
}
