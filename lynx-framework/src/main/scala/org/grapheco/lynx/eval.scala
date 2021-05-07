package org.grapheco.lynx

import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTFloat, CTInteger, CTString, CypherType}

trait ExpressionEvaluator {
  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue

  def typeOf(expr: Expression, definedVarTypes: Map[String, LynxType]): LynxType
}

case class ExpressionContext(executionContext: ExecutionContext, params: Map[String, LynxValue], vars: Map[String, LynxValue] = Map.empty) {
  def param(name: String): LynxValue = params(name)

  def var0(name: String): LynxValue = vars(name)

  def withVars(vars0: Map[String, LynxValue]): ExpressionContext = ExpressionContext(executionContext, params, vars0)
}

class DefaultExpressionEvaluator(graphModel: GraphModel, types: TypeSystem, procedures: ProcedureRegistry) extends ExpressionEvaluator {
  protected def safeBinaryOp(lhs: Expression, rhs: Expression, op: (LynxValue, LynxValue) => LynxValue)(implicit ec: ExpressionContext): LynxValue = {
    eval(lhs) match {
      case LynxNull => LynxNull
      case lvalue =>
        (lvalue, eval(rhs)) match {
          case (_, LynxNull) => LynxNull
          case (lvalue, rvalue) => op(lvalue, rvalue)
        }
    }
  }

  override def typeOf(expr: Expression, definedVarTypes: Map[String, LynxType]): LynxType = {
    expr match {
      case Parameter(name, parameterType) => parameterType

      //literal
      case _: BooleanLiteral => CTBoolean
      case _: StringLiteral => CTString
      case _: IntegerLiteral => CTInteger
      case _: DoubleLiteral => CTFloat
      case CountStar() => CTInteger

      case FunctionInvocation(Namespace(parts), FunctionName(name), distinct, args) =>
        procedures.getProcedure(parts, name).map(_.outputs.head._2).getOrElse(CTAny)

      case Variable(name) => definedVarTypes(name)
      case _ => CTAny
    }
  }


  protected def evalPathStep(step: PathStep)(implicit ec: ExpressionContext): LynxValue = {
    step match {
      case NilPathStep => LynxList(List.empty)
      case f: NodePathStep => LynxList(List(eval(f.node), evalPathStep(f.next)))
      case m: MultiRelationshipPathStep => LynxList(List(eval(m.rel), eval(m.toNode.get), evalPathStep(m.next)))
      case s: SingleRelationshipPathStep => LynxList(s.dependencies.map(eval).toList ++ List(eval(s.toNode.get)))
    }
  }

  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue =
    expr match {
      case HasLabels(expression, labels) =>
        eval(expression) match {
          case node: LynxNode => {
            val labelsNode = node.labels
            LynxBoolean(labels.forall(label => labelsNode.contains(label.name)))
          }
        }

      case pe: PathExpression => {
        evalPathStep(pe.step)
      }

      case CountStar() => LynxInteger(ec.vars.size)

      case fe: ProcedureExpression => {
        if(fe.aggregating){
          println("containsAggregate")
          LynxValue(fe.args.map(eval(_)))
        }else{
          fe.procedure.call(fe.args.map(eval(_)), ec.executionContext)
        }
      }

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
          types.wrap(lvalue != rvalue))

      case Equals(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) =>
          types.wrap(lvalue == rvalue))

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
        types.wrap(v.value)

      case v: ListLiteral =>
        LynxValue(v.expressions.map(e=>eval(e)))

      case Variable(name) =>
        ec.vars(name)

      case Property(src, PropertyKeyName(name)) =>
        eval(src) match {
          case LynxNull => LynxNull
          case cn: LynxNode => cn.property(name).getOrElse(LynxNull)
          case cr: LynxRelationship => cr.property(name).getOrElse(LynxNull)
        }

      case Parameter(name, parameterType) =>
        types.wrap(ec.param(name))

      case CaseExpression(expression, alternatives, default) => {
        val expr = alternatives.find(alt => eval(alt._1).value.asInstanceOf[Boolean]).map(_._2).getOrElse(default.get)
        eval(expr)
      }

      case MapExpression(items) => LynxMap(items.map(it => it._1.name -> eval(it._2)).toMap)
    }
}