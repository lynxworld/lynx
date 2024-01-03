package org.grapheco.lynx.physical.plans

import org.grapheco.lynx.LynxType
import org.grapheco.lynx.dataframe.DataFrame
import org.grapheco.lynx.physical._
import org.grapheco.lynx.runner.ExecutionContext
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.structural._
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions._

case class PPTSet(setItems: Seq[SetItem])(l: PhysicalPlan, val plannerContext: PhysicalPlannerContext) extends SinglePhysicalPlan(l) with WritePlan {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val records = df.records.toList // danger!
    val columnsName = df.columnsName

    /*
    setItems case:
      - SetLabelItem(Variable, Seq[LabelName])
      - SetPropertyItem(LogicalProperty, Expression)
        case expr
      - SetExactPropertiesFromMapItem(Variable, Expression) p={...}
      - SetIncludingPropertiesFromMapItem(Variable, Expression) p+={...}
     */
    def getIndex(name: String): Int = columnsName.indexOf(name) // throw

    trait SetOp

    case class CaseSet(caseExp: CaseExpression, propOp: (Map[LynxPropertyKey, LynxValue], Expression => LynxValue) => Map[LynxPropertyKey, LynxValue]) extends SetOp

    case class Label(labelOp: Seq[LynxNodeLabel] => Seq[LynxNodeLabel]) extends SetOp

    case class Static(propOp: Map[LynxPropertyKey, LynxValue] => Map[LynxPropertyKey, LynxValue]) extends SetOp

    case class Dynamic(propOp: (Map[LynxPropertyKey, LynxValue], Expression => LynxValue) => Map[LynxPropertyKey, LynxValue]) extends SetOp

    // START process for map item
    def fromMapItem(name: String, expression: Expression, including: Boolean): (Int, SetOp) = (
      getIndex(name),
      expression match {
        case l: Literal => {
          val theMap = toMap(eval(expression)(ctx.expressionContext))
          if (including) Static(_ ++ theMap)
          else Static(_ => theMap)
        }
        case p: Parameter => {
          val theMap = toMap(eval(expression)(ctx.expressionContext))
          if (including) Static(_ ++ theMap)
          else Static(_ => theMap)
        }
        case _ => if (including) Dynamic((old, evalD) => old ++ toMap(evalD(expression)))
        else Dynamic((_, evalD) => toMap(evalD(expression)))
      }
    )

    def toMap(v: LynxValue): Map[LynxPropertyKey, LynxValue] = v match {
      case m: LynxMap => m.v.map { case (str, value) => LynxPropertyKey(str) -> value }
      case h: HasProperty => h.keys.map(k => k -> h.property(k).getOrElse(LynxNull)).toMap
      case o => throw ExecuteException(s"can't find props map from type ${o.lynxType}")
    }

    //END
    /*
      Map[columnIndex, Seq[Ops]]
     */
    val ops: Map[Int, Seq[SetOp]] = setItems.map {
      case SetLabelItem(Variable(name), labels) => (getIndex(name), Label(_ ++ labels.map(_.name).map(LynxNodeLabel)))
      case SetPropertyItem(LogicalProperty(key, map), expression) =>
        (key, expression) match {
          case (Variable(name), l: Literal) => (getIndex(name), {
            val newData = eval(expression)(ctx.expressionContext)
            Static(old => old + (LynxPropertyKey(map.name) -> newData))
          })
          case (Variable(name), p: Parameter) => (getIndex(name), {
            val newData = eval(expression)(ctx.expressionContext)
            Static(old => old + (LynxPropertyKey(map.name) -> newData))
          })
          case (Variable(name), _) => (getIndex(name), Dynamic((old, evalD) => old + (LynxPropertyKey(map.name) -> evalD(expression))))
          case (ce: CaseExpression, l: Literal) => (getIndex(ce.alternatives.head._2.asInstanceOf[Variable].name), {
            val newData = eval(expression)(ctx.expressionContext)
            Dynamic((old, evalD) => evalD(ce) match {
              case LynxNull => old
              case _ => old + (LynxPropertyKey(map.name) -> newData)
            })
          })
          case (ce: CaseExpression, _) => (getIndex(ce.alternatives.head._2.asInstanceOf[Variable].name),
            Dynamic((old, evalD) => evalD(ce) match {
              case LynxNull => old
              case _ => old + (LynxPropertyKey(map.name) -> evalD(expression))
            }))
          case _ => throw ExecuteException("Unsupported type of logical property")
        }
      case SetExactPropertiesFromMapItem(Variable(name), expression) => fromMapItem(name, expression, false)
      case SetIncludingPropertiesFromMapItem(Variable(name), expression) => fromMapItem(name, expression, true)
    }.groupBy(_._1).mapValues(_.map(_._2)) //group ops of same column

    val needIndexes = ops.keySet

    DataFrame(schema, () => records.map { record =>
      record.zipWithIndex.map {
        case (e: LynxElement, index) if needIndexes.contains(index) => {
          val opsOfIt = ops(index)
          // setLabels if e is Node
          val updatedLabels = e match {
            case n: LynxNode => opsOfIt.collect { case Label(labelOp) => labelOp }
              .foldLeft(n.labels) { (labels, ops) => ops(labels) } // (default Label) => (op1) => (op2) => ... => (updated)
            case _ => Seq.empty
          }

          val updatedProps = opsOfIt.filterNot(_.isInstanceOf[Label])
            .foldLeft(e.keys.map(k => k -> e.property(k).getOrElse(LynxNull)).toMap) {
              (props, ops) =>
                ops match {
                  case Static(propOp) => propOp(props)
                  case Dynamic(propOp) => propOp(props, eval(_)(ctx.expressionContext.withVars(columnsName.zip(record).toMap)))
                }
            }

          (e match {
            case n: LynxNode => graphModel.write.updateNode(n.id, updatedLabels, updatedProps)
            case r: LynxRelationship => graphModel.write.updateRelationShip(r.id, updatedProps)
          }).getOrElse(LynxNull)
        }
        case other => other._1 // the column do not need change
      }
    }.toIterator)
  }
}
