package org.grapheco.lynx.evaluator

import org.grapheco.lynx.procedure.{ProcedureException, ProcedureExpression, ProcedureRegistry}
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property._
import org.grapheco.lynx.types.structural.{HasProperty, LynxNode, LynxNodeLabel, LynxPath, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.grapheco.lynx.types.time.LynxDateTime
import org.grapheco.lynx.types.{LynxValue, TypeSystem}
import org.grapheco.lynx.LynxType
import org.grapheco.lynx.runner.{GraphModel, NodeFilter, RelationshipFilter}
import org.grapheco.lynx.types.spatial.LynxPoint
import org.opencypher.v9_0.expressions.functions.{Collect, Id}
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.symbols.{CTAny, CTBoolean, CTFloat, CTInteger, CTList, CTString, ListType}

import scala.util.matching.Regex

/**
 * @ClassName DefaultExpressionEvaluator
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
class DefaultExpressionEvaluator(graphModel: GraphModel, types: TypeSystem, procedures: ProcedureRegistry) extends ExpressionEvaluator {
  override def typeOf(expr: Expression, definedVarTypes: Map[String, LynxType]): LynxType = {
    expr match {
      case Parameter(name, parameterType) => parameterType
      case _: BooleanLiteral => CTBoolean
      case _: StringLiteral => CTString
      case _: IntegerLiteral => CTInteger
      case _: DoubleLiteral => CTFloat
      case CountStar() => CTInteger
      case ProcedureExpression(funcInov) => funcInov.function match {
        case Collect => CTList(typeOf(funcInov.args.head, definedVarTypes))
        case Id => CTInteger
        case _ => CTAny
      }
      case ContainerIndex(expr, _) => typeOf(expr, definedVarTypes) match {
        case ListType(cypherType) => cypherType
        case _ => CTAny
      }
      case Variable(name) => definedVarTypes(name)
      case _ => CTAny
    }
  }

  protected def evalPathStep(step: PathStep)(implicit ec: ExpressionContext): LynxPath = {
    step match {
      case NilPathStep => LynxPath.EMPTY
      case f: NodePathStep => LynxPath.startPoint(eval(f.node).asInstanceOf[LynxNode]).append(evalPathStep(f.next))
      case m: MultiRelationshipPathStep => (m.rel match {
        case Variable(r) => ec.vars(r+"LINK")
        case _ => throw ProcedureException("")
      }).asInstanceOf[LynxPath]
        .append(eval(m.toNode.get).asInstanceOf[LynxNode]).append(evalPathStep(m.next))
      case s: SingleRelationshipPathStep => LynxPath.singleRel(eval(s.rel).asInstanceOf[LynxRelationship])
        .append(eval(s.toNode.get).asInstanceOf[LynxNode])
        .append(evalPathStep(s.next))
    }
  }

  private def safeBinaryOp(lhs: Expression, rhs: Expression, op: (LynxValue, LynxValue) => LynxValue)(implicit ec: ExpressionContext): Option[LynxValue] = {
    val l = eval(lhs)
    if (l.value == null) return None
    val r = eval(rhs)
    if (r.value == null) return None
    Some(op(l, r))
  }

  override def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue = {
    expr match {
      case HasLabels(expression, labels) =>
        eval(expression) match {
          case node: LynxNode => LynxBoolean(labels.forall(label => node.labels.map(_.value).contains(label.name)))
        }

      case pe: PathExpression => evalPathStep(pe.step)

      case ContainerIndex(expr, idx) => { //fixme: what's this
        {
          (eval(expr), eval(idx)) match {
            case (hp: HasProperty, i: LynxString) => hp.property(LynxPropertyKey(i.value))
            case (lm: LynxMap, key: LynxString) => lm.value.get(key.value)
            case (lm: LynxList, i: LynxInteger) => lm.value.lift(i.value.toInt)
          }
        }.getOrElse(LynxNull)
      }

      case fe: ProcedureExpression => {
        if (fe.aggregating) LynxValue(fe.args.map(eval(_)))
        else fe.procedure.execute(fe.args.map(eval(_)))
      }

      case Add(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) =>
          // TODO other cases
          (lvalue, rvalue) match {
            case (a: LynxNumber, b: LynxNumber) => a + b
            case (a: LynxString, b: LynxString) => LynxString(a.value + b.value)
            case (a: LynxString, b: LynxValue) => LynxString(a.value + b.toString)
            case (a: LynxList, b: LynxList) => LynxList(a.value ++ b.value)
          }).getOrElse(LynxNull)

      case Subtract(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) =>
          (lvalue, rvalue) match {
            case (a: LynxNumber, b: LynxNumber) => a - b
          }).getOrElse(LynxNull)

      case Ors(exprs) =>
        LynxBoolean(exprs.exists(eval(_).value == true))

      case Ands(exprs) =>
        LynxBoolean(exprs.forall(eval(_).value == true))

      case Or(lhs, rhs) =>
        LynxBoolean(eval(lhs).value == true || eval(rhs).value == true)

      case And(lhs, rhs) =>
        LynxBoolean(eval(lhs).value == true && eval(rhs).value == true)

      case sdi: IntegerLiteral => LynxInteger(sdi.value)


      case Multiply(lhs, rhs) => { //todo add normal multi
        (eval(lhs), eval(rhs)) match {
          case (n: LynxNumber, m: LynxNumber) => { //todo add aggregating multi
            (n, m) match {
              case (d1: LynxFloat, d2: LynxFloat) => LynxFloat(d1.value * d2.value)
              case (d1: LynxFloat, d2: LynxInteger) => LynxFloat(d1.value * d2.value)
              case (d1: LynxInteger, d2: LynxFloat) => LynxFloat(d1.value * d2.value)
              case (d1: LynxInteger, d2: LynxInteger) => LynxInteger(d1.value * d2.value)
            }
          }
        }
      }

      case Divide(lhs, rhs) => {
        (eval(lhs), eval(rhs)) match {
          case (n: LynxNumber, m: LynxNumber) => n / m
          case (n, m) => throw EvaluatorTypeMismatch(n.lynxType.toString, "LynxNumber")
        }
      }

      case NotEquals(lhs, rhs) => //todo add testcase: 1) n.name == null 2) n.nullname == 'some'
        LynxValue(eval(lhs) != eval(rhs))

      case Equals(lhs, rhs) => //todo add testcase: 1) n.name == null 2) n.nullname == 'some'
        LynxValue(eval(lhs) == eval(rhs))

      case GreaterThan(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) => {
          (lvalue, rvalue) match {
            // TODO: Make sure the
            case (a: LynxNumber, b: LynxNumber) => LynxBoolean(a.number.doubleValue() > b.number.doubleValue())
            case (a: LynxString, b: LynxString) => LynxBoolean(a.value > b.value)
            case _ => LynxBoolean(lvalue > rvalue)
          }
        }).getOrElse(LynxNull)

      case GreaterThanOrEqual(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) => {
          LynxBoolean(lvalue >= rvalue)
        }).getOrElse(LynxNull)

      case LessThan(lhs, rhs) =>
        eval(GreaterThan(rhs, lhs)(expr.position))

      case LessThanOrEqual(lhs, rhs) =>
        eval(GreaterThanOrEqual(rhs, lhs)(expr.position))

      case Not(in) =>
        val lynxValue: LynxValue = eval(in)
        lynxValue match {
          case LynxNull => LynxBoolean(false) //todo add testcase
          case LynxBoolean(b) => LynxBoolean(!b)
          case _ => throw EvaluatorTypeMismatch(lynxValue.lynxType.toString, "LynxBoolean")
        }

      case IsNull(lhs) => {
        eval(lhs) match {
          case LynxNull => LynxBoolean(true)
          case _ => LynxBoolean(false)
        }
      }
      case IsNotNull(lhs) => {
        eval(lhs) match {
          case LynxNull => LynxBoolean(false)
          case _ => LynxBoolean(true)
        }
      }

      case v: Literal =>
        types.wrap(v.value)

      case v: ListLiteral =>
        LynxValue(v.expressions.map(eval(_)))

      case Variable(name) =>
        ec.vars(name)

      case Property(src, PropertyKeyName(name)) =>
        eval(src) match {
          case LynxNull => LynxNull
          case hp: HasProperty => hp.property(LynxPropertyKey(name)).getOrElse(LynxNull)
          case time: LynxDateTime => LynxValue(name match { //TODO add HasProperty into LynxDateTime and remove this case.
            case "epochMillis" => time.epochMillis
          })
          case map: LynxMap => map.get(name).getOrElse(LynxNull)
        }

      case In(lhs, rhs) =>
        eval(rhs) match {
          case LynxList(list) => LynxBoolean(list.contains(eval(lhs))) //todo add literal in list[func] test case
        }

      case Parameter(name, parameterType) =>
        types.wrap(ec.param(name))

      case RegexMatch(lhs, rhs) => {
        (eval(lhs), eval(rhs)) match {
          case (LynxString(str), LynxString(regStr)) => {
            val regex = new Regex(regStr) // TODO: opt
            val res = regex.findFirstMatchIn(str)
            if (res.isDefined) LynxBoolean(true)
            else LynxBoolean(false)
          }
          case (LynxNull, _) => LynxBoolean(false)
        }
      }

      case StartsWith(lhs, rhs) => {
        (eval(lhs), eval(rhs)) match {
          case (LynxString(str), LynxString(startStr)) => LynxBoolean(str.startsWith(startStr))
          case (LynxNull, _) => LynxBoolean(false)
        }
      }

      case EndsWith(lhs, rhs) => {
        (eval(lhs), eval(rhs)) match {
          case (LynxString(str), LynxString(endStr)) => LynxBoolean(str.endsWith(endStr))
          case (LynxNull, _) => LynxBoolean(false)
        }
      }

      case Contains(lhs, rhs) => {
        (eval(lhs), eval(rhs)) match {
          case (LynxString(str), LynxString(containsStr)) => LynxBoolean(str.contains(containsStr))
          case (LynxNull, _) => LynxBoolean(false)
        }
      }

      case CaseExpression(expression, alternatives, default) => {
        if (expression.isDefined) {
          val evalValue = eval(expression.get)
          evalValue match {
            case LynxNull => LynxNull
            case _ => {
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
        else {
          val expr = alternatives.find(alt => eval(alt._1).value.asInstanceOf[Boolean]).map(_._2).getOrElse {
            default.orNull
          }
          if (expr != null) eval(expr)
          else LynxNull
        }
      }

      case MapExpression(items) => LynxMap(items.map(it => it._1.name -> eval(it._2)).toMap)

      //Only One-hop path-pattern is supported now
      case PatternExpression(pattern) => { // TODO
        val rightNode: NodePattern = pattern.element.rightNode
        val relationship: RelationshipPattern = pattern.element.relationship
        val leftNode: NodePattern = if (pattern.element.element.isSingleNode) {
          pattern.element.element.asInstanceOf[NodePattern]
        } else {
          throw EvaluatorException(s"PatternExpression is not fully supproted.")
        }

//        val exist: Boolean = graphModel.paths(
//          _transferNodePatternToFilter(leftNode),
//          _transferRelPatternToFilter(relationship),
//          _transferNodePatternToFilter(rightNode),
//          relationship.direction, 1, 1
//        ).filter(path => if (leftNode.variable.nonEmpty) path.startNode.compareTo(eval(leftNode.variable.get)) == 0 else true)
//          .filter(path => if (rightNode.variable.nonEmpty) path.endNode.compareTo(eval(rightNode.variable.get)) == 0 else true).nonEmpty

        val exist = false
        LynxBoolean(exist)
      }

      case ip: IterablePredicateExpression => {
        val variable = ip.variable
        val predicate = ip.innerPredicate
        val predicatePass: ExpressionContext => Boolean = if (predicate.isDefined) {
          ec => eval(predicate.get)(ec) == LynxBoolean.TRUE
        } else { _ => true } // if predicate not defined, should must return true?

        eval(ip.expression) match {
          case list: LynxList => {
            val ecList = list.v.map(i => ec.withVars(ec.vars + (variable.name -> i)))
            val result = ip match {
              case _: AllIterablePredicate => ecList.forall(predicatePass)
              case _: AnyIterablePredicate => ecList.exists(predicatePass)
              case _: NoneIterablePredicate => ecList.forall(predicatePass.andThen(!_))
              case _: SingleIterablePredicate => ecList.indexWhere(predicatePass) match {
                case -1 => false // none
                case i => !ecList.drop(i + 1).exists(predicatePass) // only one!
              }
            }
            LynxBoolean(result)
          }
          case _ => throw ProcedureException("The expression must returns a list.")
        }
      }

      case Pow(lhs, rhs) => (eval(lhs), eval(rhs)) match {
        case (number: LynxNumber, exponent: LynxNumber) => LynxFloat(Math.pow(number.toDouble, exponent.toDouble))
        case _ => throw ProcedureException("The expression must returns tow numbers.")
      }

      case ListSlice(list, from, to) => eval(list) match {
        case LynxList(list) => LynxList((from.map(eval), to.map(eval)) match {
          case (Some(LynxInteger(i)), Some(LynxInteger(j))) => list.slice(i.toInt, j.toInt)
          case (Some(LynxInteger(i)), _) => list.drop(i.toInt)
          case (_, Some(LynxInteger(j))) => list.slice(0, j.toInt)
          case (_, _) => throw ProcedureException("The range must is a integer.")
        })
        case _ => throw ProcedureException("The expression must returns a list.")
      }

      case ReduceExpression(scope, init, list) => {
        val variableName = scope.variable.name
        val accumulatorName = scope.accumulator.name
        var accumulatorValue = eval(init)
        eval(list) match {
          case list: LynxList => {
            list.v.foreach(listValue => accumulatorValue = eval(scope.expression)(ec.withVars(ec.vars ++ Map(variableName -> listValue, accumulatorName -> accumulatorValue)))
            )
            accumulatorValue
          }
          case _ => throw ProcedureException("The expression must returns a list.")
        }
      }

      case ListComprehension(scope, expression) => {
        val variableName = scope.variable.name
        eval(expression) match {
          case list: LynxList => {
            var result = LynxList(List())

            if (scope.extractExpression.isDefined) {
              result = list.map {
                listValue =>
                  eval(scope.extractExpression.get)(ec.withVars(ec.vars + (variableName -> listValue)))
              }
            }

            if (scope.innerPredicate.isDefined) {
              result = LynxList(list.v.filter {
                listValue => eval(scope.innerPredicate.get)(ec.withVars(ec.vars + (variableName -> listValue))).asInstanceOf[LynxBoolean].value
              })
            }
            result
          }
          case _ => throw ProcedureException("The expression must returns a list.")
        }
      }

    }
  }

  override def aggregateEval(expr: Expression)(ecs: Seq[ExpressionContext]): LynxValue = {
    expr match {
      case fe: ProcedureExpression =>
        if (fe.aggregating) {
          val listArgs = {
            if (fe.distinct){
              LynxList(ecs.map(eval(fe.args.head)(_)).distinct.toList)
            } else {
              LynxList(ecs.map(eval(fe.args.head)(_)).toList)
            }
          } //todo: ".head": any multi-args situation?
          val otherArgs = fe.args.drop(1).map(eval(_)(ecs.head)) // 2022.09.15: Added handling of other args, but the default first one is list
          fe.procedure.execute(Seq(listArgs) ++ otherArgs)
        } else {
          throw ProcedureException("aggregate by nonAggregating procedure.")
        }
      case CountStar() => LynxInteger(ecs.length)
    }
  }

  def _transferNodePatternToFilter(nodePattern: NodePattern)(implicit ec: ExpressionContext): NodeFilter = {
    val properties: Map[LynxPropertyKey, LynxValue] = nodePattern.properties match {
      case None => Map()
      case Some(MapExpression(seqOfProps)) => seqOfProps.map {
        case (propertyKeyName, propValueExpr) => LynxPropertyKey(propertyKeyName.name) -> LynxValue(eval(propValueExpr))
      }.toMap
    }
    NodeFilter(nodePattern.labels.map(label => LynxNodeLabel(label.name)), properties)
  }

  def _transferRelPatternToFilter(relationshipPattern: RelationshipPattern)(implicit ec: ExpressionContext): RelationshipFilter = {
    val props: Map[LynxPropertyKey, LynxValue] = relationshipPattern.properties match {
      case None => Map()
      case Some(MapExpression(seqOfProps)) => seqOfProps.map {
        case (propertyKeyName, propValueExpr) => LynxPropertyKey(propertyKeyName.name) -> LynxValue(eval(propValueExpr))
      }.toMap
    }
    RelationshipFilter(relationshipPattern.types.map(relType => LynxRelationshipType(relType.name)), props)
  }

}
