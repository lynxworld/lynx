package org.grapheco.lynx.evaluator

import org.grapheco.lynx.optimizer.ApplyPushDownRule
import org.grapheco.lynx.optimizer.PPTFilterPushDownRule.{extractIndexPropFromFilterExpression, foldPushDown, getNewPattern, getPushDownExpression, reverseRelationPattern}
import org.grapheco.lynx.procedure.{ProcedureException, ProcedureExpression, ProcedureRegistry}
import org.grapheco.lynx.runner.filter.FilterExpr
import org.grapheco.lynx.runner.{GraphModel, NodeFilter, RelationshipFilter, filter}
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property._
import org.grapheco.lynx.types.structural._
import org.grapheco.lynx.types.time._
import org.grapheco.lynx.types.traits.{HasProperty, LynxComputable}
import org.grapheco.lynx.types.{CT2LT, LTAny, LTBoolean, LTFloat, LTInteger, LTList, LTString, LazyLynxValue, LynxType, LynxValue, TypeSystem}
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.expressions.functions.{Collect, Id}
import org.opencypher.v9_0.util.InputPosition
import org.opencypher.v9_0.util.symbols.ListType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.abs
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
      case _: BooleanLiteral => LTBoolean
      case _: StringLiteral => LTString
      case _: IntegerLiteral => LTInteger
      case _: DoubleLiteral => LTFloat
      case CountStar() => LTInteger
      case ProcedureExpression(funcInov) => funcInov.function match {
        case Collect => LTList(typeOf(funcInov.args.head, definedVarTypes))
        case Id => LTInteger
        case _ => LTAny
      }
      case ContainerIndex(expr, _) => typeOf(expr, definedVarTypes) match {
        case ListType(cypherType) => cypherType
        case _ => LTAny
      }
      case Variable(name) => definedVarTypes(name)
      case _ => LTAny
    }
  }

  protected def evalPathStep(step: PathStep)(implicit ec: ExpressionContext): LynxPath = {
    step match {
      case NilPathStep => LynxPath.EMPTY
      case f: NodePathStep => {
        val path = f.next match {
          case m: MultiRelationshipPathStep => LynxPath.EMPTY
          case _ => LynxPath.startPoint(eval(f.node).asInstanceOf[LynxNode])
        }
        path.append(evalPathStep(f.next))
      }
      case m: MultiRelationshipPathStep => (m.rel match {
        case Variable(r) => ec.vars(r + "LINK")
        case _ => throw ProcedureException("")
      }).asInstanceOf[LynxPath]
        //.append(eval(m.toNode.get).asInstanceOf[LynxNode])
        //.append(evalPathStep(m.next))
      case s: SingleRelationshipPathStep => LynxPath.singleRel(
          eval(s.rel) match {
            case r: LynxRelationship => r
            case o: LynxList => o.v.head.asInstanceOf[LynxRelationship]
          })
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

  def judge(value: LynxValue): Boolean = value match {
    case LynxList(l) => l.nonEmpty
    case LynxBoolean(v) => v
    case LynxNull => false
    case o => throw EvaluatorTypeMismatch(o.lynxType.toString, "Boolean")
  }

  override def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue = {

    val value: LynxValue = expr match {
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
            case (lm: LynxList, i: LynxInteger) =>
              if (i.value.toInt < 0)
                lm.value.reverse.lift(abs(i.value.toInt) - 1)
              else
                lm.value.lift(i.value.toInt)
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
            case (a: LynxLocalDateTime, b: LynxDuration) => a.plusDuration(b)
            case (a: LynxDate, b: LynxDuration) => a.plusDuration(b)
            case (a: LynxTime, b: LynxDuration) => a.plusDuration(b)
            case (a: LynxDuration, b: LynxDuration) => a.plusByMap(b)
            case (a: LynxDateTime, b: LynxDuration) => a.plusDuration(b)
            case (a: LynxComputable, b: LynxComputable) => a add b
          }).getOrElse(LynxNull)

      case Subtract(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) =>
          (lvalue, rvalue) match {
            case (a: LynxNumber, b: LynxNumber) => a - b
            case (a: LynxLocalDateTime, b: LynxDuration) => a.minusDuration(b)
            case (a: LynxDate, b: LynxDuration) => a.minusDuration(b)
            case (a: LynxDuration, b: LynxDuration) => a.minusByMap(b)
            case (a: LynxTime, b: LynxDuration) => a.minusDuration(b)
            case (a: LynxDateTime, b: LynxDuration) => a.minusDuration(b)
            case (a: LynxComputable, b: LynxComputable) => a subtract  b
          }).getOrElse(LynxNull)

      case Ors(exprs) => LynxBoolean(exprs.map(eval(_)).exists(judge))

      case Ands(exprs) => LynxBoolean(exprs.map(eval).forall(judge))

      case Or(lhs, rhs) => LynxBoolean(judge(eval(lhs)) || judge(eval(rhs)))

      case And(lhs, rhs) => LynxBoolean(judge(eval(lhs)) && judge(eval(rhs)))

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
          case (d1: LynxDuration, d2: LynxInteger) => d1.multiplyInt(d2)
          case (a: LynxComputable, b: LynxComputable) => a multiply  b
          case (n, m) => throw EvaluatorOptUnsupported(n.lynxType.toString, m.lynxType.toString, expr.toString)
        }
      }

      case Divide(lhs, rhs) =>
        (eval(lhs), eval(rhs)) match {
          case (n: LynxNumber, m: LynxNumber) => n / m
          case (n: LynxDuration, m: LynxInteger) => n.divideInt(m)
          case (a: LynxComputable, b: LynxComputable) => a divide  b
          case (n, m) => throw EvaluatorOptUnsupported(n.lynxType.toString, m.lynxType.toString, expr.toString)
        }

      case Modulo(lhs, rhs) => {
        (eval(lhs), eval(rhs)) match {
          case (n: LynxInteger, m: LynxInteger) => {
            n % m
          }
          case (n, m) => throw EvaluatorTypeMismatch(n.lynxType.toString, "LynxInteger")
        }
      }

      case NotEquals(lhs, rhs) => eval(Equals(lhs, rhs)(expr.position)) match {
        case LynxBoolean(v) => LynxBoolean(!v)
        case LynxNull => LynxNull
      }

      case Equals(lhs, rhs) => (eval(lhs), eval(rhs)) match {
        case (LynxNull, _) => LynxNull
        case (_, LynxNull) => LynxNull
        case (l, r) =>
          //          TODO Aggregate function sum（LynxInteger） => LynxInteger
          val r1 = r match {
            case v:LynxInteger => LynxFloat(v.value.toDouble)
            case _ => r
          }
          val l1 = l match {
            case v:LynxInteger => LynxFloat(v.value.toDouble)
            case _ => l
          }
          LynxBoolean(l1 == r1)
      }

      case GreaterThan(lhs, rhs) =>
        safeBinaryOp(lhs, rhs, (lvalue, rvalue) => {
          (lvalue, rvalue) match {
            // TODO: Make sure the
            case (a: LynxNumber, b: LynxNumber) => LynxBoolean(a.number.doubleValue() > b.number.doubleValue())
            case (a: LynxString, b: LynxString) => LynxBoolean(a.value > b.value)
            case _ => if (lvalue.getClass != rvalue.getClass) LynxNull else LynxBoolean(lvalue > rvalue)
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

      case Not(in) => LynxBoolean(!judge(eval(in)))

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
          //TODO Temporary modification
          case l: LynxList => val r = l.value.map(v => v match {
            case hp: HasProperty => hp.property(LynxPropertyKey(name)).getOrElse(LynxNull)
            case _ => LynxNull
          }).filter(_!=LynxNull)
            if(r.length == 1) r.head else LynxList(r)
          case _ => LynxNull
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
                  if (res.isInstanceOf[LynxBoolean]) res.value.asInstanceOf[Boolean] == evalValue
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

      case MapExpression(items) => LynxMap(items.map { case (prop, expr) => prop.name -> eval(expr) }.toMap)


      case PatternExpression(pattern) => executePattern(pattern, None, true)

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
        case (number: LynxInteger, exponent: LynxInteger) => LynxInteger(Math.pow(number.value, exponent.value).toLong)
        case (number: LynxNumber, exponent: LynxNumber) => LynxFloat(Math.pow(number.toDouble, exponent.toDouble))
        case _ => throw ProcedureException("The expression must returns tow numbers.")
      }

      case ListSlice(list, from, to) => eval(list) match {
        case LynxList(list) => LynxList((from.map(eval), to.map(eval)) match {
          case (Some(LynxInteger(i)), Some(LynxInteger(j))) =>
            val left = if (i.toInt < 0) list.length + i.toInt else i.toInt
            val right = if (j.toInt < 0) list.length + j.toInt else j.toInt
            list.slice(left, right)
          case (Some(LynxInteger(i)), _) =>
            val idx = if (i.toInt < 0) list.length + i.toInt else i.toInt
            list.drop(idx)
          case (_, Some(LynxInteger(j))) =>
            val right = if (j.toInt < 0) list.length + j.toInt else j.toInt
            list.slice(0, right)
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
            var result = list
            if (scope.innerPredicate.isDefined) {
              result = LynxList(list.v.filter {
                listValue => val value = eval(scope.innerPredicate.get)(ec.withVars(ec.vars + (variableName -> listValue)))
                  value match {
                    case l@LynxList(list) => list.nonEmpty
                    case l@LynxBoolean(v) => v
                    case _ => false
                  }
              })
            }

            if (scope.extractExpression.isDefined) {
              result = result.map {
                listValue =>
                  eval(scope.extractExpression.get)(ec.withVars(ec.vars + (variableName -> listValue)))
              }
            }

            result
          }
          case _ => throw ProcedureException("The expression must returns a list.")
        }
      }

      case DesugaredMapProjection(name, items, includeAllProps) => LynxMap(items.map(item => item.key.name -> eval(item.exp)(ec)).toMap)

      /*
        eg: [(a)-[r:ACTION_IN]->(b) WHERE b:Movie | b.released]
        namedPath: None
        pattern: (a)-[r:ACTION_IN]->(b)
        predicate: HasLabels(b, Movie)
        projection: Property(b, released)
       */
      case PatternComprehension(namedPath: Option[LogicalVariable], pattern: RelationshipsPattern,
      predicate: Option[Expression], projection: Expression) => {
        // TODO
        val listValue = LynxList(executePattern(pattern, predicate)(ec).value.map(v =>eval(projection)))
        listValue
      }
    }
    LazyLynxValue.initLazyLynxValue(value)
  }

  override def aggregateEval(expr: Expression)(ecs: Iterator[ExpressionContext]): LynxValue = {
    expr match {
      case fe: ProcedureExpression =>
        val ecsSeq = ecs.toSeq
//        TODO Is the eval method thread safe
        if (fe.aggregating) {
          val listArgs = {
            if (fe.distinct) {
              LynxList(ecsSeq.par.map(eval(fe.args.head)(_)).distinct.toList)
            } else {
              LynxList(ecsSeq.par.map(eval(fe.args.head)(_)).toList)
            }
          } //todo: ".head": any multi-args situation?
          val otherArgs = fe.args.drop(1).map(eval(_)(ecsSeq.head)) // 2022.09.15: Added handling of other args, but the default first one is list
          fe.procedure.execute(Seq(listArgs) ++ otherArgs)
        } else {
          throw ProcedureException("aggregate by nonAggregating procedure.")
        }
      case CountStar() => LynxInteger(ecs.length)
    }
  }

  private def _transferNodePatternToFilter(nodePattern: NodePattern)(implicit ec: ExpressionContext): NodeFilter = {
    val filterExpr: Option[FilterExpr] = nodePattern.properties match {
      case None => None
      case pn@Some(ListLiteral(list)) => Some(filter.Ands(list.map(toFilerExpr(_)).toSet))
    }

    NodeFilter(nodePattern.labels.map(label => LynxNodeLabel(label.name)), Map.empty, filterExpr)
  }

  def toFilerExpr(expression: Expression)(implicit ec: ExpressionContext): FilterExpr = {
    expression match {
      case e@Equals(lhs, rhs) => lhs match {
        case Property(Variable(name), pkn) => filter.Equals(LynxPropertyKey(pkn.name), LynxValue(eval(rhs)))
        case Variable(name) => eval(rhs) match {
          case n: LynxNode => filter.Equals(LynxPropertyKey("_lynx_sys_id"), n.id.toLynxInteger)
          case _ => throw new Exception(s"transferNodePatternToFilter fail ${e}")
        }
        case p: ProcedureExpression => filter.Equals(LynxPropertyKey("_lynx_sys_id"), LynxValue(eval(rhs)))
      }
      case in@In(lhs, rhs) => lhs match {
        case Property(Variable(name), pkn) => eval(rhs) match {
          case l: LynxList => filter.In(LynxPropertyKey(pkn.name), l)
          case _ => throw new Exception(s"transferNodePatternToFilter fail ${in}")
        }
        case Variable(name) => eval(rhs) match {
          case LynxList(l: List[LynxNode]) => filter.In(LynxPropertyKey("_lynx_sys_id"), LynxList(l.map(_.id.toLynxInteger)))
          case _ => throw new Exception(s"transferNodePatternToFilter fail ${in}")
        }
        case p: ProcedureExpression => eval(rhs) match {
          case l: LynxList => filter.In(LynxPropertyKey("_lynx_sys_id"), l)
          case _ => throw new Exception(s"transferNodePatternToFilter fail ${in}")
        }
      }
      case _ => throw new Exception(s"transferNodePatternToFilter fail ${expression}")
    }
  }
  private def _transferRelPatternToFilter(relationshipPattern: RelationshipPattern)(implicit ec: ExpressionContext): RelationshipFilter = {
    val props: Map[LynxPropertyKey, LynxValue] = relationshipPattern.properties match {
      case None => Map()
      case Some(MapExpression(seqOfProps)) => seqOfProps.map {
        case (propertyKeyName, propValueExpr) => LynxPropertyKey(propertyKeyName.name) -> LynxValue(eval(propValueExpr))
      }.toMap
    }
    RelationshipFilter(relationshipPattern.types.map(relType => LynxRelationshipType(relType.name)), props)
  }

  private def pushFilterToPattern(pattern: RelationshipsPattern, expr: Option[Expression], isAddSysId: Boolean)
                                 (implicit ec: ExpressionContext): Seq[(ArrayBuffer[NodePattern], ArrayBuffer[RelationshipPattern], ArrayBuffer[String], Option[Expression], Boolean)] = {
    var notPushDown: ArrayBuffer[Expression] = new ArrayBuffer[Expression]
    var pushDown: ArrayBuffer[Seq[(String, Expression)]] = new ArrayBuffer[Seq[(String, Expression)]]
    var labelMap = mutable.Map[String, Seq[LabelName]]()
    if(expr.nonEmpty){
      val pushDownExpr = getPushDownExpression(expr.get, ec.executionContext.physicalPlannerContext)
      notPushDown = pushDownExpr._1
      pushDown = pushDownExpr._2
      labelMap = pushDownExpr._3
    }
    var pushDowns: Seq[Seq[(String, Expression)]] = foldPushDown(pushDown)
    if(pushDowns.isEmpty) pushDowns = Seq(Seq.empty)
    pushDowns.map(pushDown => {
      val schema = new ArrayBuffer[String]()
      val nodeArr = new ArrayBuffer[NodePattern]()
      val relArr = new ArrayBuffer[RelationshipPattern]()
      var relationshipChain: PatternElement = pattern.element
      var isReverse = false
      while (!relationshipChain.isSingleNode) {
        val rel = relationshipChain.asInstanceOf[RelationshipChain]
        if(expr.isEmpty) {
          nodeArr.append(rel.rightNode)
        }else {
          nodeArr.append(getNewPattern(rel.rightNode, Seq(pushDown), labelMap).head)
        }
        relArr.append(rel.relationship)
        schema.append({
          if(rel.rightNode.variable.nonEmpty) rel.rightNode.variable.get.name
          else rel.rightNode.toString
        })
        schema.append({
          if(rel.relationship.variable.nonEmpty) rel.relationship.variable.get.name
          else rel.relationship.toString
        })
        if (rel.element.isSingleNode) {
          if(expr.isEmpty){
            val startNodePattern = rel.element.asInstanceOf[NodePattern]
            if(startNodePattern.properties.isEmpty) isReverse = true
            nodeArr.append(startNodePattern)
          }else {
            val startNodePattern = getNewPattern(rel.element.asInstanceOf[NodePattern], Seq(pushDown), labelMap).head
            if(startNodePattern.properties.isEmpty) isReverse = true
            nodeArr.append(startNodePattern)
          }
          schema.append({
            val node = rel.element.asInstanceOf[NodePattern]
            if(node.variable.nonEmpty) node.variable.get.name
            else node.toString
          })
        }
        relationshipChain = rel.element
      }
      val notPutDownExpr = if(notPushDown.nonEmpty) Some(Ands(notPushDown.toSet)(InputPosition(0,0,0))) else None
      if(isReverse && !isAddSysId) (nodeArr.reverse, relArr.map(rel => reverseRelationPattern(rel)).reverse, schema.reverse, notPutDownExpr, isReverse)
      else (nodeArr, relArr, schema, notPutDownExpr, isReverse)
    })
  }

  private def executePattern(pattern: RelationshipsPattern, expr: Option[Expression], isAddSysId: Boolean = false)(implicit ec: ExpressionContext): LynxList = {
    val patterns = pushFilterToPattern(pattern, expr, isAddSysId)
    val result = patterns.map(pattern => {
      val (nodeArr, relArr, schema, notPutDownExpr, isReverse) = pattern
      var nodeIter: Iterator[NodePattern] = Iterator.empty
      if(isAddSysId){
        nodeIter = nodeArr.map(ApplyPushDownRule.addIdToNodePattern(_)(ec.executionContext.physicalPlannerContext)).reverse.toIterator
      }else nodeIter = nodeArr.reverse.toIterator
      val relIter = relArr.reverse.toIterator
      var resultPaths: Seq[Seq[LynxValue]] = Seq.empty
      if (relIter.hasNext) {
        val relationship = relIter.next()
        val leftNode = nodeIter.next()
        val rightNode = nodeIter.next()
        resultPaths = graphModel.paths(
          _transferNodePatternToFilter(leftNode),
          _transferRelPatternToFilter(relationship),
          _transferNodePatternToFilter(rightNode),
          relationship.direction, 1, 1
        ).map(_.elements).toSeq
      }
      while (relIter.hasNext) {
        val relationship = relIter.next()
        val endNodePattern = nodeIter.next()
        val endNodeFilter = _transferNodePatternToFilter(endNodePattern)
        val resultExpands = resultPaths.flatMap(record => {
          graphModel.varExpandWithLabel(record.last.asInstanceOf[LynxNode], _transferRelPatternToFilter(relationship), relationship.direction, 1, 1)
            .filter(_.endNode.forall(endNodeFilter.matches(_)))
            .map(p => record.:+(p.relationships.head).:+(p.endNode.get))

        })
        resultPaths = resultExpands
      }
      val pushDownList: List[LynxList] = if(notPutDownExpr.nonEmpty){
        val filterResult:List[LynxList] = resultPaths.par.filter{
          (record: Seq[LynxValue]) =>
            try{
              val r1 = eval(notPutDownExpr.get)(ec.withVars(ec.vars ++ schema.reverse.zip(record).toMap))
              val r2 = r1 match {
                case LynxBoolean(b) => b
                case LynxList(l) => l.nonEmpty
                case LynxNull => false //todo check logic
              }
              r2
            }catch {
              case e: Exception => println(record)
                println(schema)
                throw e
            }


        }.toList.map(_.map(v=> v.asInstanceOf[LynxElement]).toList).map(LynxList(_))
        filterResult
      }else resultPaths.toList.map(_.map(v=> v.asInstanceOf[LynxElement]).toList).map(LynxList(_))
      if(isReverse) pushDownList.map(lynxList => LynxList(lynxList.value.reverse))
      else pushDownList
    }).reduceOption(_ ++ _).getOrElse(List.empty[LynxList])
    LynxList(result)
  }
}
