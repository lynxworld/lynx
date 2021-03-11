package org.grapheco.lynx

import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTFloat, CTInteger, CTString}

trait ExpressionEvaluator {
  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue

  def typeOf(expr: Expression, definedVarTypes: Map[String, LynxType]): LynxType
}

case class ExpressionContext(params: Map[String, LynxValue], vars: Map[String, LynxValue] = Map.empty) {
  def param(name: String): LynxValue = params(name)

  def var0(name: String): LynxValue = vars(name)

  def withVars(vars0: Map[String, LynxValue]): ExpressionContext = ExpressionContext(params, vars0)
}

class ExpressionEvaluatorImpl(graphModel: GraphModel) extends ExpressionEvaluator {
  private def safeBinaryOp(lhs: Expression, rhs: Expression, op: (LynxValue, LynxValue) => LynxValue)(implicit ec: ExpressionContext): LynxValue = {
    eval(lhs) match {
      case LynxNull => LynxNull
      case lvalue =>
        (lvalue, eval(rhs)) match {
          case (_, LynxNull) => LynxNull
          case (lvalue, rvalue) => op(lvalue, rvalue)
        }
    }
  }

  override def typeOf(expr: Expression, definedVarTypes: Map[String, LynxType]): LynxType =
    expr match {
      case Parameter(name, parameterType) => parameterType

      //literal
      case _: BooleanLiteral => CTBoolean
      case _: StringLiteral => CTString
      case _: IntegerLiteral => CTInteger
      case _: DoubleLiteral => CTFloat

      case Variable(name) => definedVarTypes(name)
      case _ => CTAny
    }


  def evalStep(step: PathStep)(implicit ec: ExpressionContext): LynxValue ={
/*    step match {
      case NilPathStep => LynxNull
      case f: NodePathStep => LynxList(List(eval(f.node),evalStep(f.next)))
      case m: MultiRelationshipPathStep => LynxList(List(eval(m.rel), eval(m.toNode.get), evalStep(m.next)))
      case s: SingleRelationshipPathStep => LynxList(List(eval(s.rel), evalStep(s.next)))
    }*/
    step match {
      case NilPathStep => LynxList(List.empty)
      case f: NodePathStep =>   LynxList( List(eval(f.node), evalStep(f.next)))
      case m: MultiRelationshipPathStep =>   LynxList(List(eval(m.rel), eval(m.toNode.get), evalStep(m.next)))
      case s: SingleRelationshipPathStep => LynxList(s.dependencies.map(eval).toList)
    }


  }

  override def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue =
    expr match {
      case HasLabels(expression, labels) =>
        eval(expression) match {
          case node: LynxNode => {
            val labelsNode = node.labels
            LynxBoolean(labels.forall(label => labelsNode.contains(label.name)))
          }
        }


      case pe:PathExpression => {

        evalStep(pe.step)
        //LynxList(pe.step.dependencies.map(eval).toList)
      /*  pe.step match {
          case NilPathStep => LynxNull
          case f: NodePathStep => LynxList(List(eval(f.node)) ++ f.dependencies.map(eval).toList)
          case m: MultiRelationshipPathStep => LynxList(List(eval(m.rel)) ++ m.dependencies.map(eval).toList)
          case s: SingleRelationshipPathStep => LynxList(s.dependencies.map(eval).toList)
        }*/
      }
      case f:FunctionInvocation => LynxFunction.getValue(f.namespace.parts, f.functionName.name, f.args.map(eval), graphModel)


      case Add(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) =>
          (lvalue, rvalue) match {
            case (a: LynxNumber, b: LynxNumber) => a + b
            case (a: LynxString, b: LynxString) => LynxString(a.v + b.v)
          })

      case Subtract(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) =>
          (lvalue, rvalue) match {
            case (a: LynxNumber, b: LynxNumber) => a - b
          })

      case Ors(exprs) =>
        LynxBoolean(exprs.exists(eval(_) == LynxBoolean(true)))

      case Ands(exprs) =>
        LynxBoolean(exprs.forall(eval(_) == LynxBoolean(true)))

      case Or(lhs, rhs) =>
        LynxBoolean(Seq(lhs, rhs).exists(eval(_) == LynxBoolean(true)))

      case And(lhs, rhs) =>
        LynxBoolean(Seq(lhs, rhs).forall(eval(_) == LynxBoolean(true)))

      case NotEquals(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) =>
          LynxValue(lvalue != rvalue))

      case Equals(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) =>
          LynxValue(lvalue == rvalue))

      case GreaterThan(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) => {
          (lvalue, rvalue) match {
            case (a: LynxNumber, b: LynxNumber) => LynxBoolean(a.number.doubleValue() > b.number.doubleValue())
          }
        })

      case GreaterThanOrEqual(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) => {
          (lvalue, rvalue) match {
            case (a: LynxNumber, b: LynxNumber) => LynxBoolean(a.number.doubleValue() >= b.number.doubleValue())
          }
        })

      case LessThan(lhs, rhs) =>
        eval(GreaterThan(rhs, lhs)(expr.position))

      case LessThanOrEqual(lhs, rhs) =>
        eval(GreaterThanOrEqual(rhs, lhs)(expr.position))

      case Not(in) =>
        eval(in) match {
          case LynxNull => LynxNull
          case LynxBoolean(b) => LynxBoolean(!b)
        }

      case v: Literal =>
        LynxValue(v.value)

      case Variable(name) =>
        ec.vars(name)

      case Property(src, PropertyKeyName(name)) =>
        eval(src) match {
          case LynxNull => LynxNull
          case cn: LynxNode => cn.property(name).get
          case cr: LynxRelationship => cr.property(name).get
        }

      case Parameter(name, parameterType) =>
        LynxValue(ec.param(name))
      case CaseExpression(expression, alternatives, default) => {
        val expr = alternatives.find(alt=>eval(alt._1).value.asInstanceOf[Boolean]).map(_._2).getOrElse(default.get)
        eval(expr)
      }
      case MapExpression(items) => LynxMap(items.map(it => it._1.name->eval(it._2)).toMap)
    }
}