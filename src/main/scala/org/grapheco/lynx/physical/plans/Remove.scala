package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.opencypher.v9_0.ast.{RemoveItem, RemoveLabelItem, RemovePropertyItem}

case class Remove(removeItems: Seq[RemoveItem])(l: PhysicalPlan, val plannerContext: PhysicalPlannerContext) extends SinglePhysicalPlan(l) {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val res = df.records.map(n => {
      n.size match {
        case 1 => {
          var tmpNode: LynxNode = n.head.asInstanceOf[LynxNode]
          removeItems.foreach {
            case rp@RemovePropertyItem(property) =>
              tmpNode = graphModel.removeNodesProperties(Iterator(tmpNode.id), Array(rp.property.propertyKey.name)).next().get

            case rl@RemoveLabelItem(variable, labels) =>
              tmpNode = graphModel.removeNodesLabels(Iterator(tmpNode.id), rl.labels.map(f => f.name).toArray).next().get
          }
          Seq(tmpNode)
        }
        case 3 => {
          var triple: Seq[LynxValue] = n
          removeItems.foreach {
            case rp@RemovePropertyItem(property) => {
              val newRel = graphModel.removeRelationshipsProperties(Iterator(triple(1).asInstanceOf[LynxRelationship].id), Array(property.propertyKey.name)).next().get
              triple = Seq(triple.head, newRel, triple.last)
            }

            case rl@RemoveLabelItem(variable, labels) => {
              // TODO: An relation is able to have multi-type ???
              val newRel = graphModel.removeRelationshipType(Iterator(triple(1).asInstanceOf[LynxRelationship].id), labels.map(f => f.name).toArray.head).next().get
              triple = Seq(triple.head, newRel, triple.last)
            }
          }
          triple
        }
      }
    })
    DataFrame.cached(schema, res.toSeq)
  }
}
