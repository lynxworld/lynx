package org.grapheco.lynx

import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.expressions.functions.Count
import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTFloat, CTInteger, CTString, CypherType}


//codebaby add CTType of aggreFunction
trait AggregationType{

}

sealed abstract class CountStarType extends CypherType with AggregationType
object CountStarType {
  val instance = new CountStarType() {
    val parentType = CTAny
    override val toString = "CountStar"
    override val toNeoTypeString = "CountStar()"
  }
}
sealed abstract class CountType extends CypherType with AggregationType
object CountType {
  val instance = new CountType() {
    val parentType = CTAny
    override val toString = "Count"
    override val toNeoTypeString = "Count()"
  }
}

sealed abstract class SumType extends CypherType with AggregationType
object SumType {
  val instance = new SumType() {
    val parentType = CTAny
    override val toString = "Sum"
    override val toNeoTypeString = "Sum()"
  }
}
sealed abstract class MaxType extends CypherType with AggregationType
object MaxType {
  val instance = new MaxType() {
    val parentType = CTAny
    override val toString = "Max"
    override val toNeoTypeString = "Max()"
  }
}

sealed abstract class MinType extends CypherType with AggregationType
object MinType {
  val instance = new MinType() {
    val parentType = CTAny
    override val toString = "Min"
    override val toNeoTypeString = "Min()"
  }
}
sealed abstract class AvgType extends CypherType with AggregationType
object AvgType {
  val instance = new AvgType() {
    val parentType = CTAny
    override val toString = "Avg"
    override val toNeoTypeString = "Avg()"
  }
}

object ExpandType{
  val CTCountStar: CountStarType = CountStarType.instance
  val CTCount: CountType = CountType.instance
  val CTSum: SumType = SumType.instance
  val CTMax: MaxType = MaxType.instance
  val CTMin: MinType = MinType.instance
  val CTAvg: AvgType = AvgType.instance
}
//codebaby add CTType of aggreFunction


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
      case CountStar() => ExpandType.CTCountStar
      case FunctionInvocation(namespace, functionName, distinct, args) => functionName.name.toLowerCase match{
        case "count" => ExpandType.CTCount
        case "sum" => ExpandType.CTSum
        case "max" => ExpandType.CTMax
        case "min" => ExpandType.CTMin
        case "avg" => ExpandType.CTAvg
        case _ => CTAny
      }


      case Variable(name) => definedVarTypes(name)
      case _ => CTAny
    }


  def evalStep(step: PathStep)(implicit ec: ExpressionContext): LynxValue ={
    step match {
      case NilPathStep => LynxList(List.empty)
      case f: NodePathStep =>   LynxList( List(eval(f.node), evalStep(f.next)))
      case m: MultiRelationshipPathStep =>   LynxList(List(eval(m.rel), eval(m.toNode.get), evalStep(m.next)))
      case s: SingleRelationshipPathStep => LynxList(s.dependencies.map(eval).toList ++ List(eval(s.toNode.get)))
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


      case pe: PathExpression => {
        evalStep(pe.step)
      }

      case FunctionInvocation(Namespace(parts), FunctionName(name), distinct, args) => name.toLowerCase match {
        case "sum" => eval(args.head)
        case "max" => eval(args.head)
        case "min" => eval(args.head)
        case "count" => LynxInteger(1)
        case _ =>LynxFunction.getValue(parts, name, args.map(eval), graphModel)
      }

      case CountStar() => LynxInteger(ec.vars.size)


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
          case cn: LynxNode => cn.property(name).getOrElse(LynxNull)
          case cr: LynxRelationship => cr.property(name).getOrElse(LynxNull)
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