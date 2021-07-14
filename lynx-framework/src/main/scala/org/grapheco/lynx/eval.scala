package org.grapheco.lynx

import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTFloat, CTInteger, CTString, CypherType}

import scala.util.matching.Regex

trait ExpressionEvaluator {
  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue

  def evalGroup(expr: Expression)(ecs: Seq[ExpressionContext]): LynxValue

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

      case pe: ProcedureExpression =>
        pe.procedure.outputs.head._2

      case Variable(name) => definedVarTypes(name)
      case _ => CTAny
    }
  }


  protected def evalPathStep(step: PathStep)(implicit ec: ExpressionContext): LynxValue = {
    step match {
      case NilPathStep => LynxList(List.empty)
      case f: NodePathStep => LynxList(List(eval(f.node), evalPathStep(f.next)))
      case m: MultiRelationshipPathStep => LynxList(List(eval(m.rel), eval(m.toNode.get), evalPathStep(m.next)))
      case s: SingleRelationshipPathStep =>
        LynxList(s.dependencies.map(eval).toList ++ List(eval(s.toNode.get)) ++ List(evalPathStep(s.next)))
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

        // bugs
      case ContainerIndex(expr, idx) =>{
        (expr, idx) match {
          case (v@Variable(name), pe@ProcedureExpression(funcInov)) =>{
            ec.vars(name) match {
              case node: LynxNode => node.property(eval(pe).value.asInstanceOf[LynxString].value).getOrElse(LynxNull)
              case rel: LynxRelationship => rel.property(eval(pe).value.asInstanceOf[LynxString].value).getOrElse(LynxNull)
            }
          }
        }
      }

      case fe: ProcedureExpression => {
        if(fe.aggregating){
          LynxValue(fe.args.map(eval(_)))
        }else{
          fe.procedure.call(fe.args.map(eval(_)))
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
      case Multiply(lhs, rhs) =>{
        (lhs, rhs) match {
          case (pe@ProcedureExpression(funcInov), sdi@SignedDecimalIntegerLiteral(stringVal)) =>{
            LynxDouble(eval(pe).value.asInstanceOf[Double] * sdi.value)
          }
        }
      }
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
            case (a: LynxString, b: LynxString) => LynxBoolean(a.value > b.value)
          }
        })

      case GreaterThanOrEqual(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) => {
          (lvalue, rvalue) match {
            case (a: LynxNumber, b: LynxNumber) => LynxBoolean(a.number.doubleValue() >= b.number.doubleValue())
            case (a: LynxString, b: LynxString) => LynxBoolean(a.value >= b.value)
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

      case IsNull(lhs) =>{
        eval(lhs) match {
          case LynxNull => LynxBoolean(true)
          case _ => LynxBoolean(false)
        }
      }
      case IsNotNull(lhs) =>{
        eval(lhs) match {
          case LynxNull => LynxBoolean(false)
          case _ => LynxBoolean(true)
        }
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
          case time: LynxDateTime => time
        }

      case In(lhs, rhs) =>{
        rhs match {
          case list@ListLiteral(expressions) => {
            lhs match {
              case pe@ProcedureExpression(funcInov) =>
                LynxBoolean(eval(list).asInstanceOf[LynxList].value.contains(eval(pe).value))
              case prop@Property(map, propertyKey) =>
                LynxBoolean(eval(list).asInstanceOf[LynxList].value.contains(eval(prop)))
            }
          }
        }
      }

      case Parameter(name, parameterType) =>
        types.wrap(ec.param(name))

      case RegexMatch(lhs, rhs) =>{
        (lhs, rhs) match {
          case (pe@ProcedureExpression(funcInov), StringLiteral(value)) =>{
            val regex = new Regex(value) // TODO: opt
            val res = regex.findFirstMatchIn(eval(pe).value.toString)
            if (res.isDefined) LynxBoolean(true)
            else LynxBoolean(false)
          }
          case (Property(map, propertyKey), StringLiteral(value)) =>{
            val regex = new Regex(value) // TODO: opt
            map match {
              case Variable(name) =>{
                ec.vars(name) match {
                  case node: LynxNode => {
                    val res = regex.findFirstMatchIn(node.property(propertyKey.name).getOrElse("").toString)
                    if (res.isDefined) LynxBoolean(true)
                    else LynxBoolean(false)
                  }
                  case rel: LynxRelationship => {
                    val res = regex.findFirstMatchIn(rel.property(propertyKey.name).getOrElse("").toString)
                    if (res.isDefined) LynxBoolean(true)
                    else LynxBoolean(false)
                  }
                }
              }
            }
          }
        }
      }
      case StartsWith(lhs, rhs) =>{
        (lhs, rhs) match {
          case (Property(map, propertyKey), StringLiteral(value)) =>{
            map match {
              case Variable(name) =>{
                ec.vars(name) match {
                  case node: LynxNode => LynxBoolean(node.property(propertyKey.name).getOrElse(LynxString("")).value.toString.startsWith(value))
                  case rel: LynxRelationship => LynxBoolean(rel.property(propertyKey.name).getOrElse(LynxString("")).value.toString.startsWith(value))
                }
              }
            }
          }
        }
      }
      case EndsWith(lhs, rhs) =>{
        (lhs, rhs) match {
          case (Property(map, propertyKey), StringLiteral(value)) =>{
            map match {
              case Variable(name) =>{
                ec.vars(name) match {
                  case node: LynxNode => LynxBoolean(node.property(propertyKey.name).getOrElse(LynxString("")).value.toString.endsWith(value))
                  case rel: LynxRelationship => LynxBoolean(rel.property(propertyKey.name).getOrElse(LynxString("")).value.toString.endsWith(value))
                }
              }
            }
          }
        }
      }
      case Contains(lhs, rhs) =>{
        (lhs, rhs) match {
          case (Property(map, propertyKey), StringLiteral(value)) =>{
            map match {
              case Variable(name) =>{
                ec.vars(name) match {
                  case node: LynxNode => LynxBoolean(node.property(propertyKey.name).getOrElse(LynxString("")).value.toString.contains(value))
                  case rel: LynxRelationship => LynxBoolean(rel.property(propertyKey.name).getOrElse(LynxString("")).value.toString.contains(value))
                }
              }
            }
          }
        }
      }
      case CaseExpression(expression, alternatives, default) => {
        if (expression.isDefined){
          val evalValue = eval(expression.get)
          evalValue match {
            case LynxNull => LynxNull
            case _ =>{
              val expr = alternatives.find(
                alt => {
                  // case [xxx] when [yyy] then 1
                  // if [yyy] is a boolean, then [xxx] no use
                  val res = eval(alt._1)
                  if (res.isInstanceOf[LynxBoolean]) res.value.asInstanceOf[Boolean]
                  else eval(alt._1) == evalValue
                })
                .map(_._2).getOrElse(default.get)

              eval(expr)
            }
          }
        }
        else{
          val expr = alternatives.find(alt => eval(alt._1).value.asInstanceOf[Boolean]).map(_._2).getOrElse{default.orNull}
          if (expr != null) eval(expr)
          else LynxNull
        }
      }
      case MapExpression(items) => LynxMap(items.map(it => it._1.name -> eval(it._2)).toMap)
    }

  override def evalGroup(expr: Expression)(ecs: Seq[ExpressionContext]): LynxValue = {
    val vls = LynxValue(ecs.map(eval(expr)(_).value.asInstanceOf[Seq[LynxValue]].head))//TODO fix head
    expr match {
      case fe: ProcedureExpression => fe.procedure.call(Seq(vls))
    }
  }
}