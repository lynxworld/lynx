package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.types.{LTNode, LTPath, LTRelationship, LynxType, LynxValue}
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.{PhysicalPlannerContext, SyntaxErrorException}
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.opencypher.v9_0.expressions.Expression
/**
 * The DELETE clause is used to delete graph elements — nodes, relationships or paths.
 *
 * @param delete
 * @param in
 * @param plannerContext
 */
case class Delete(expressions: Seq[Expression], forced: Boolean)(implicit val plannerContext: PhysicalPlannerContext) extends SinglePhysicalPlan {

  override def schema: Seq[(String, LynxType)] = Seq.empty

  override def execute(implicit ctx: ExecutionContext): DataFrame = { // TODO so many bugs !
    val df = in.execute(ctx)
    expressions foreach { exp =>
      val projected = df.project(Seq(("delete", exp)))(ctx.expressionContext)
      val (_, elementType) = projected.schema.head
      elementType match {
        case LTNode => graphModel.deleteNodesSafely(
          dropNull(projected.records) map {
            _.asInstanceOf[LynxNode].id
          }, forced)
        case LTRelationship => graphModel.deleteRelations(
          dropNull(projected.records) map {
            _.asInstanceOf[LynxRelationship].id
          })
        case LTPath =>
        case _ => throw SyntaxErrorException(s"expected Node, Path pr Relationship, but a ${elementType}")
      }
    }

    def dropNull(values: Iterator[Seq[LynxValue]]): Iterator[LynxValue] =
      values.flatMap(_.headOption.filterNot(LynxNull.equals))

    DataFrame.empty
  }

}
