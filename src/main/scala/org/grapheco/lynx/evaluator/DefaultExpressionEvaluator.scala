package org.grapheco.lynx.evaluator

import org.grapheco.lynx.procedure.{ProcedureException, ProcedureExpression, ProcedureRegistry}
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property._
import org.grapheco.lynx.types.structural.{HasProperty, LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.lynx.types.time.LynxDateTime
import org.grapheco.lynx.types.{LynxValue, TypeSystem}
import org.grapheco.lynx.LynxType
import org.grapheco.lynx.runner.{GraphModel, NodeFilter, RelationshipFilter}
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


  protected def evalPathStep(step: PathStep)(implicit ec: ExpressionContext): LynxValue = {
    step match {
      case NilPathStep => LynxList(List.empty)
      case f: NodePathStep => LynxList(List(eval(f.node), evalPathStep(f.next)))
      case m: MultiRelationshipPathStep => LynxList(List(eval(m.rel), eval(m.toNode.get), evalPathStep(m.next)))
      case s: SingleRelationshipPathStep =>
        LynxList(s.dependencies.map(eval).toList ++ List(eval(s.toNode.get)) ++ List(evalPathStep(s.next)))
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

      case fe: ProcedureExpression => { //TODO move aggregating to other place
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
          case (n: LynxNumber, m: LynxInteger) => { //todo add aggregating multi
            n match {
              case d: LynxFloat => LynxFloat(d.value * m.value)
              case d: LynxInteger => LynxInteger(d.value * m.value)
            }
          }
        }
      }

      case Divide(lhs, rhs) => {
        (eval(lhs), eval(rhs)) match {
          case (n: LynxNumber, m: LynxNumber) => n / m
          case (n,m) => throw EvaluatorTypeMismatch(n.lynxType.toString,"LynxNumber")
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
          case _ => throw EvaluatorTypeMismatch(lynxValue.lynxType.toString, "LynxBoolean" )
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
          case time: LynxDateTime => time
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
      case PatternExpression(pattern) => {
        val rightNode: NodePattern = pattern.element.rightNode
        val relationship: RelationshipPattern = pattern.element.relationship
        val leftNode: NodePattern = if (pattern.element.element.isSingleNode) {
          pattern.element.element.asInstanceOf[NodePattern]
        } else {
          throw EvaluatorException(s"PatternExpression is not fully supproted.")
        }

        val exist: Boolean = graphModel.paths(
          _transferNodePatternToFilter(leftNode),
          _transferRelPatternToFilter(relationship),
          _transferNodePatternToFilter(rightNode),
          relationship.direction, Some(1), Some(1)
        ).filter(path => if(leftNode.variable.nonEmpty) path.startNode.compareTo(eval(leftNode.variable.get)) == 0 else true)
          .filter(path => if(rightNode.variable.nonEmpty) path.endNode.compareTo(eval(rightNode.variable.get)) == 0 else true).nonEmpty

        LynxBoolean(exist)
      }


    }




  }


  override def aggregateEval(expr: Expression)(ecs: Seq[ExpressionContext]): LynxValue = {
    expr match {
      case fe: ProcedureExpression =>
        if (fe.aggregating) {
          val listArgs = LynxList(ecs.map(eval(fe.args.head)(_)).toList) //todo: ".head": any multi-args situation?
          val otherArgs = fe.args.drop(1).map(eval(_)(ecs.head)) // 2022.09.15: Added handling of other args, but the default first one is list
          fe.procedure.execute( Seq(listArgs) ++ otherArgs)
        } else {
          throw ProcedureException("aggregate by nonAggregating procedure.")
        }
      case CountStar() => LynxInteger(ecs.length)

    }

  }

  def _transferNodePatternToFilter(nodePattern: NodePattern)(implicit ec: ExpressionContext): NodeFilter = {
    val properties: Map[LynxPropertyKey, LynxValue] = nodePattern.properties match {
      case None => Map()
      case Some(MapExpression(seqOfProps)) => seqOfProps.map{
        case (propertyKeyName, propValueExpr) => LynxPropertyKey(propertyKeyName.name) -> LynxValue(eval(propValueExpr))
      }.toMap
    }
    NodeFilter(nodePattern.labels.map(label => LynxNodeLabel(label.name)), properties)
  }

  def _transferRelPatternToFilter(relationshipPattern: RelationshipPattern)(implicit ec: ExpressionContext): RelationshipFilter = {
    val props: Map[LynxPropertyKey, LynxValue] = relationshipPattern.properties match {
      case None => Map()
      case Some(MapExpression(seqOfProps)) => seqOfProps.map{
        case (propertyKeyName, propValueExpr) => LynxPropertyKey(propertyKeyName.name) -> LynxValue(eval(propValueExpr))
      }.toMap
    }
    RelationshipFilter(relationshipPattern.types.map(relType => LynxRelationshipType(relType.name)),props)
  }

}
