package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.evaluator.ExpressionContext
import org.grapheco.lynx.physical._
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.structural._
import org.grapheco.lynx.types.traits.HasProperty
import org.opencypher.v9_0.ast.{SetItem, SetLabelItem, SetPropertyItem, SetExactPropertiesFromMapItem, SetIncludingPropertiesFromMapItem}
import org.opencypher.v9_0.expressions._

case class Set(setItems: Seq[SetItem])(implicit val plannerContext: PhysicalPlannerContext) extends SinglePhysicalPlan with WritePlan {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val records = df.records.toList // danger! TODO may cause OOM
    val columnNames = df.columnsName

    /*
    setItems case:
      - SetLabelItem(Variable, Seq[LabelName])
      - SetPropertyItem(LogicalProperty, Expression)
        case expr
      - SetExactPropertiesFromMapItem(Variable, Expression) p={...}
      - SetIncludingPropertiesFromMapItem(Variable, Expression) p+={...}
     */
    val ops: Map[Int, Seq[SetOperator]] = setItems.map( _ match {
      case i: SetLabelItem => new SetLabelOperator(i)
      case SetPropertyItem(LogicalProperty(colExpr, propKeyName), propValueExpr) => new AddSinglePropertyOperator(false, colExpr, propKeyName, propValueExpr)
      case SetIncludingPropertiesFromMapItem(Variable(colName), propValueExpr) => new SetMultiplePropertiesOperator(propValueExpr, false, colName)
      case SetExactPropertiesFromMapItem(Variable(colName), propValueExpr) => new SetMultiplePropertiesOperator(propValueExpr, true, colName)
    }).groupBy(_.colIndex(columnNames))

    val needIndexes = ops.keySet

    DataFrame.cached(schema,  records.map { record =>
      record.zipWithIndex.map {
        case (e: LynxElement, index) if needIndexes.contains(index) =>
          ops(index).map(_.perform(e, record, columnNames, ctx.expressionContext)).last.getOrElse(LynxNull)
        case other => other._1 // the column do not need change
      }
    })
  }

  private trait SetOperator {
    protected def getColName(): String
    def colIndex(colNames: Seq[String]): Int = colNames.indexOf(getColName)
    def perform(e: LynxElement, record: Seq[LynxValue], colNames: Seq[String], ec: ExpressionContext): Option[LynxElement]
  }

  private class SetLabelOperator(sl: SetLabelItem) extends SetOperator {
    override def getColName(): String = sl.variable.name
    override def perform(e: LynxElement, record: Seq[LynxValue], colNames: Seq[String], ec: ExpressionContext): Option[LynxElement] = {
      graphModel.write.setNodesLabels(Some(e.id).iterator, sl.labels.map(_.name).map(LynxNodeLabel).toArray).next()
    }
  }

  private abstract class SetPropertyOperator(val cleanExistProperties: Boolean) extends SetOperator{
    protected def evalPropertyData(record: Seq[LynxValue], colNames: Seq[String], ec: ExpressionContext): Array[(LynxPropertyKey, LynxValue)]
    override def perform(e: LynxElement, record: Seq[LynxValue], colNames: Seq[String], ec: ExpressionContext): Option[LynxElement] = {
      val setFunc = e match {
        case _: LynxNode => graphModel.write.setNodesProperties(_, _, cleanExistProperties)
        case _: LynxRelationship => graphModel.write.setRelationshipsProperties(_, _, cleanExistProperties)
      }
      val it = Some(e.id).iterator
      val propData = evalPropertyData(record, colNames, ec)
      setFunc(it, propData).next()
    }
  }

  private class AddSinglePropertyOperator(override val cleanExistProperties: Boolean, colExpr: Expression, propKeyName: PropertyKeyName, propValueExpr: Expression) extends SetPropertyOperator(cleanExistProperties) {
    override def getColName(): String = {
      colExpr match {
        case Variable(name) => name
        case ce: CaseExpression => ce.alternatives.head._2.asInstanceOf[Variable].name
        case _ => throw ExecuteException("Unsupported type of logical property")
      }
    }
    override protected def evalPropertyData(record: Seq[LynxValue], colNames: Seq[String], ec: ExpressionContext): Array[(LynxPropertyKey, LynxValue)] = {
      val newPropValue: Option[LynxValue] = (colExpr, propValueExpr) match {
          case (Variable(_), _: Literal | _: Parameter) => Some(eval(propValueExpr)(ec))
          case (Variable(_) | _: CaseExpression, _) =>
            val newEC = ec.withVars(colNames.zip(record).toMap)
            colExpr match {
              case Variable(_) => Some(eval(propValueExpr)(newEC))
              case ce: CaseExpression =>
                eval(ce)(newEC) match {
                  case LynxNull => None
                  case _ => Some(eval(propValueExpr)(newEC))
                }
            }
          case _ => throw ExecuteException("Unsupported type of logical property")
      }
      newPropValue.map(v => Array((LynxPropertyKey(propKeyName.name), v))).getOrElse(Array.empty)
    }
  }

  private class SetMultiplePropertiesOperator(val propValueExpr: Expression, override val cleanExistProperties: Boolean, colName: String) extends SetPropertyOperator(cleanExistProperties) {
    override protected def getColName(): String = colName
    override protected def evalPropertyData(record: Seq[LynxValue], colNames: Seq[String], ec: ExpressionContext): Array[(LynxPropertyKey, LynxValue)] = {
      val nec = propValueExpr match {
        case _: Literal | _: Parameter => ec
        case _ => ec.withVars(colNames.zip(record).toMap)
      }
      val mapData = eval(propValueExpr)(nec) match {
        case m: LynxMap => m.v.map { case (str, value) => LynxPropertyKey(str) -> value }
        case h: HasProperty => h.keys.map(k => k -> h.property(k).getOrElse(LynxNull)).toMap
        case o => throw ExecuteException(s"can't find props map from type ${o.lynxType}")
      }
      mapData.toArray
    }
  }
}