 package org.grapheco.lynx

import java.util.regex.Pattern
import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.func.{LynxProcedure, LynxProcedureArgument}
import org.grapheco.lynx.types.{LynxValue, TypeSystem}
import org.grapheco.lynx.util.{LynxDateTimeUtil, LynxDateUtil, LynxDurationUtil, LynxLocalDateTimeUtil, LynxLocalTimeUtil, LynxTimeUtil}
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.{LynxBoolean, LynxDouble, LynxInteger, LynxNull, LynxNumber, LynxString}
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.lynx.types.time.{LynxDate, LynxDateTime, LynxDuration, LynxLocalDateTime, LynxLocalTime, LynxTime}
import org.opencypher.v9_0.expressions.{Expression, FunctionInvocation}
import org.opencypher.v9_0.util.InputPosition

import java.time.Duration
import scala.collection.mutable

trait CallableProcedure {
  val inputs: Seq[(String, LynxType)]
  val outputs: Seq[(String, LynxType)]

  def call(args: Seq[LynxValue]): LynxValue

  def signature(procedureName: String) = s"$procedureName(${inputs.map(x => Seq(x._1, x._2).mkString(":")).mkString(",")})"

  def checkArguments(procedureName: String, argTypesActual: Seq[LynxType]) = {
    if (argTypesActual.size != inputs.size)
      throw WrongNumberOfArgumentsException(s"$procedureName(${inputs.map(x => Seq(x._1, x._2).mkString(":")).mkString(",")})",
        inputs.size, argTypesActual.size)

    inputs.zip(argTypesActual).foreach(x => {
      if (x._1._2 != x._2)
        throw WrongArgumentException(x._1._1, x._2, x._1._2)
    })
  }
}

trait ProcedureRegistry {
  def getProcedure(prefix: List[String], name: String, argsLength: Int): Option[CallableProcedure]
}

class DefaultProcedureRegistry(types: TypeSystem, classes: Class[_]*) extends ProcedureRegistry with LazyLogging {
  val procedures = mutable.Map[(String, Int), CallableProcedure]()

  classes.foreach(registerAnnotatedClass(_))

  def registerAnnotatedClass(clazz: Class[_]): Unit = {
    val host = clazz.newInstance()
    clazz.getDeclaredMethods.foreach(met => {
      val an = met.getAnnotation(classOf[LynxProcedure])
      //yes, you are a LynxFunction
      if (an != null) {
        //input arguments
        val inputs = met.getParameters.map(par => {
          val pan = par.getAnnotation(classOf[LynxProcedureArgument])
          val argName =
            if (pan == null) {
              par.getName
            }
            else {
              pan.name()
            }

          argName -> types.typeOf(par.getType)
        })

        //TODO: N-tuples
        val outputs = Seq("value" -> types.typeOf(met.getReturnType))
        register(an.name(), inputs, outputs, (args) => types.wrap(met.invoke(host, args: _*)))
      }
    })
  }

  def register(name: String, argsLength: Int, procedure: CallableProcedure): Unit = {
    procedures((name, argsLength)) = procedure
    logger.debug(s"registered procedure: ${procedure.signature(name)}")
  }

  def register(name: String, inputs0: Seq[(String, LynxType)], outputs0: Seq[(String, LynxType)], call0: (Seq[LynxValue]) => LynxValue): Unit = {
    register(name, inputs0.size, new CallableProcedure() {
      override val inputs: Seq[(String, LynxType)] = inputs0
      override val outputs: Seq[(String, LynxType)] = outputs0

      override def call(args: Seq[LynxValue]): LynxValue = LynxValue(call0(args))
    })
  }

  override def getProcedure(prefix: List[String], name: String, argsLength: Int): Option[CallableProcedure] = procedures.get(((prefix :+ name).mkString("."), argsLength))
}

case class UnknownProcedureException(prefix: List[String], name: String) extends LynxException {
  override def getMessage: String = s"unknown procedure: ${(prefix :+ name).mkString(".")}"
}

case class WrongNumberOfArgumentsException(signature: String, sizeExpected: Int, sizeActual: Int) extends LynxException {
  override def getMessage: String = s"Wrong number of arguments of $signature(), expected: $sizeExpected, actual: ${sizeActual}."
}

case class WrongArgumentException(argName: String, expectedType: LynxType, actualType: LynxType) extends LynxException {
  override def getMessage: String = s"Wrong argument of $argName, expected: $expectedType, actual: ${actualType}."
}

 case class LynxProcedureException(msg: String) extends LynxException {
   override def getMessage: String = msg
 }

 case class ProcedureExpression(val funcInov: FunctionInvocation)(implicit runnerContext: CypherRunnerContext) extends Expression with LazyLogging {
  val procedure: CallableProcedure = runnerContext.procedureRegistry.getProcedure(funcInov.namespace.parts, funcInov.functionName.name, funcInov.args.size).getOrElse(throw ProcedureUnregisteredException(funcInov.name))
  val args: Seq[Expression] = funcInov.args
  val aggregating: Boolean = funcInov.containsAggregate

  logger.debug(s"binding FunctionInvocation ${funcInov.name} to procedure ${procedure}, containsAggregate: ${aggregating}")

  override def position: InputPosition = funcInov.position

  override def productElement(n: Int): Any = funcInov.productElement(n)

  override def productArity: Int = funcInov.productArity

  override def canEqual(that: Any): Boolean = funcInov.canEqual(that)

  override def containsAggregate: Boolean = funcInov.containsAggregate

  override def findAggregate: Option[Expression] = funcInov.findAggregate

}

class DefaultProcedures {

  val booleanPattern = Pattern.compile("true|false", Pattern.CASE_INSENSITIVE)
  val numberPattern = Pattern.compile("-?[0-9]+.?[0-9]*")

  @LynxProcedure(name = "lynx")
  def lynx(): String = {
    "lynx-0.3"
  }

  ///////////////////////////////////////////////////////////////////////////
  // Predicate functions
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Returns true if the specified property exists in the node, relationship or map.
   * @param property A property (in the form 'variable.prop')
   * @return A Boolean
   */
  @LynxProcedure(name = "exists")
  def exists(property: LynxValue): Boolean = {
    property match {
      case list: LynxList => ??? // TODO how to judge a list?
      case _ => property.value != null
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Scalar functions
  ///////////////////////////////////////////////////////////////////////////

//  @LynxProcedure(name = "coalesce")
//  def coalesce()

//  /**
//   * Returns the end node of a relationship.
//   * Considerations:
//   *   endNode(null) returns null
//   * @param relationship An expression that returns a relationship.
//   * @return A Node.
//   */
//  @LynxProcedure(name = "endNode")
//  def endNode(relationship: LynxRelationship): LynxNode = {
//
//  }


  /**
   * Returns the first element in a list.
   * Considerations:
   * - head(null) returns null.
   * - If the first element in list is null, head(list) will return null.
   * @param list An expression that returns a list.
   * @return The type of the value returned will be that of the first element of list.
   */
  @LynxProcedure(name = "head")
  def head(list: LynxList): LynxValue = {
    list.v.headOption.getOrElse(LynxNull)
  }

  /**
   * Returns the id of a relationship or node.
   * Considerations:
   * - id(null) returns null.
   * @param x An expression that returns a node or a relationship.
   * @return An Integer
   */
  @LynxProcedure(name = "id")
  def id(x: LynxValue): LynxInteger = x match {
    case n: LynxNode => n.id.toLynxInteger
    case r: LynxRelationship => r.id.toLynxInteger
    case _ => throw LynxProcedureException("id() can only used on node or relationship.")
  }

  /**
   * Returns the last element in a list.
   * Considerations:
   * - last(null) returns null.
   * - If the last element in list is null, last(list) will return null.
   * @param list An expression that returns a list.
   * @return The type of the value returned will be that of the last element of list.
   */
  @LynxProcedure(name = "last")
  def last(list: LynxList): LynxValue = {
    list.v.lastOption.getOrElse(LynxNull)
  }

  /**
   * Returns the length of a path.
   * Considerations:
   * - length(null) returns null.
   * @param path An expression that returns a path.
   * @return An Integer
   */
  @LynxProcedure(name = "length")
  def length(path: LynxList): LynxInteger = { //fixme how to calculate the length of a path
    LynxInteger(path.v.size)
  }

  /**
   * Returns a map containing all the properties of a node or relationship.
   * If the argument is already a map, it is returned unchanged.
   * Consideration:
   * - properties(null) returns null
   * @param x An expression that returns a node, a relationship, or a map.
   * @return A Map
   */
  @LynxProcedure(name = "properties")
  def properties(x: LynxValue): LynxMap = x match {
    case n: LynxNode => LynxMap(n.keys.map( k => k.value -> n.property(k).getOrElse(LynxNull)).toMap)
    case r: LynxRelationship => LynxMap(r.keys.map( k => k.value -> r.property(k).getOrElse(LynxNull)).toMap)
    case m: LynxMap => m
    case _ => throw LynxProcedureException("properties() can only used on node, relationship or map.")
  }

  /**
   * Returns the number of elements in a list.
   * Considerations:
   * - size(null) returns null.
   * @param list An expression that returns a list.
   * @return An Integer.
   */
  @LynxProcedure(name = "size")
  def size(list: LynxList): LynxInteger = {
    LynxInteger(list.value.size)
  }

  // TODO size() applied to pattern expression

  /**
   * size() applied to string: returns the size of a string value
   * @param string An expression that returns a string value.
   * @return An Integer.
   */
  @LynxProcedure(name = "size")
  def size(string: LynxString): LynxInteger = {
    LynxInteger(string.value.length)
  }

//  @LynxProcedure(name = "startNode")
//  def startNode(lynxRelationship: LynxRelationship): LynxNode = {
//
//  }

  /**
   * Will return the same value during one entire query, even for long-running queries.
   * TODO how to ensure same value during one entire query?
   * @return An Integer
   */
  @LynxProcedure(name = "timestamp")
  def timestamp(): LynxInteger = {
    LynxInteger(System.currentTimeMillis())
  }

  /**
   * Converts a string value to a boolean value.
   * Considerations:
   * - toBoolean(null returns null.
   * - if expression is a boolean value, it will be returned unchanged.
   * - if the parsing fails, null will be returned.
   * @param x An expression that returns a boolean or string value.
   * @return A Boolean
   */
  @LynxProcedure(name = "toBoolean")
  def toBoolean(x: LynxValue): LynxValue = {
    x match {
      case LynxString(str) => {
        val res = booleanPattern.matcher(str)
        if (res.matches()) LynxValue(str.toBoolean)
        else LynxNull
      }
      case b: LynxBoolean => b
      case _ => throw LynxProcedureException("toBoolean conversion failure")
    }
  }

  /**
   * Converts an integer or string value to a floating point number.
   * Considerations:
   * - toFloat(null) returns null
   * - If expression is a floating point number, it will be returned unchanged.
   * - If the parsing fails, null will be returned
   * @param x An expression that returns a numeric or string value.
   * @return A Float
   */
  @LynxProcedure(name = "toFloat")
  def toFloat(x: LynxValue): LynxValue = {
    x match {
      case d: LynxDouble => d
      case i: LynxInteger => LynxDouble(i.number.doubleValue())
      case r: LynxString => {
        val str = r.value
        val res = numberPattern.matcher(str)
        if (res.matches()) LynxDouble(str.toDouble)
        else LynxNull
      }
      case _ => throw LynxProcedureException("toFloat conversion failure")
    }
  }

  /**
   * Converts a floating point or string value to an integer value
   * COnsiderations:
   * - toInteger(null) returns null.
   * - If expression is an integer value, it will be returned unchanged.
   * - If the parsing fails, null will be returned.
   * @param x An expression that returns a numeric or string value.
   * @return An Integer.
   */
  @LynxProcedure(name = "toInteger")
  def toInteger(x: LynxValue): LynxValue = {
    x match {
      case i: LynxInteger => i
      case d: LynxDouble => LynxInteger(d.number.intValue())
      case LynxString(str) =>
        if (numberPattern.matcher(str).matches()) LynxInteger(str.toDouble.toInt)
        else LynxNull
      case _ => throw LynxProcedureException("toInteger conversion failure")
    }
  }

  /**
   * Returns the string representation of the relationship type.
   * @param x An expression that returns a relationship.
   * @return A String
   */
  @LynxProcedure(name = "type")
  def getType(x: LynxRelationship): LynxString = {
    x.relationType.map(_.value).map(LynxString).getOrElse(LynxString(""))
//    x match {
//      case r: LynxRelationship => {
//        val t = r.relationType
//        if (t.isDefined) LynxValue(t.get)
//        else LynxNull
//      }
//      case _ => throw new LynxProcedureException("type can only used on relationship")
//    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Aggregating functions
  // These functions take multiple values as arguments, and calculate and return an aggregated value from them.
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Returns the average of a set of numeric values
   * Considerations:
   * - avg(null) returns null
   * @param inputs An expression returning a set of numeric values.
   * @return Either an Integer or a Float, depending on the values
   *         returned by expression and whether or not the calculation overflows
   */
  @LynxProcedure(name = "avg")
  def avg(inputs: LynxList): LynxValue = {
    val dropNull = inputs.value.filterNot(LynxNull.equals)
    val firstIsNum = dropNull.headOption.map{
      case _ :LynxNumber => true
      case _ :LynxDuration => false
      case v :LynxValue => throw LynxProcedureException(s"avg() can only handle numerical values, duration, and null. Got ${v}")
    }
    if (firstIsNum.isDefined) {
      var numSum = 0.0
      var durSum = Duration.ZERO
      dropNull.foreach{ v =>
        if (v.isInstanceOf[LynxNumber] || v.isInstanceOf[LynxDuration]) {
          if (v.isInstanceOf[LynxNumber] == firstIsNum.get) {
            if (firstIsNum.get) { numSum += v.asInstanceOf[LynxNumber].number.doubleValue()}
            else { durSum = durSum.plus(v.asInstanceOf[LynxDuration].duration)}
          } else { throw LynxProcedureException("avg() cannot mix number and duration")}
        } else { throw LynxProcedureException(s"avg() can only handle numerical values, duration, and null. Got ${v}")}
      }
      if (firstIsNum.get) { LynxDouble(numSum / dropNull.length)}
      else { LynxDuration(durSum.dividedBy(dropNull.length))}
    } else { LynxNull }
  }

  /**
   * Returns a list containing the values returned by an expression.
   * Using this function aggregates data by amalgamating multiple records or values into a single list
   * @param inputs An expression returning a set of values.
   * @return A list containing heterogeneous elements;
   *         the types of the elements are determined by the values returned by expression.
   */
  @LynxProcedure(name = "collect")
  def collect(inputs: LynxList): LynxList = { //TODO other considerations
    inputs
  }

  /**
   * Returns the number of values or records, and appears in two variants:
   * @param inputs An expression
   * @return An Integer
   */
  @LynxProcedure(name = "count") //TODO count() is complex
  def count(inputs: LynxList): LynxInteger = {
    LynxInteger(inputs.value.length)
  }

  /**
   * Returns the maximum value in a set of values.
   * @param inputs An expression returning a set containing any combination of property types and lists thereof.
   * @return A property type, or a list, depending on the values returned by expression.
   */
  @LynxProcedure(name = "max")
  def max(inputs: LynxList): LynxValue = {
    inputs.max
  }

  /**
   * Returns the minimum value in a set of values.
   * @param inputs An expression returning a set containing any combination of property types and lists thereof.
   * @return A property type, or a list, depending on the values returned by expression.
   */
  @LynxProcedure(name = "min")
  def min(inputs: LynxList): LynxValue = {
    inputs.min
  }

  /**
   *  returns the percentile of the given value over a group, with a percentile from 0.0 to 1.0.
   *  It uses a linear interpolation method, calculating a weighted average between two values if
   *  the desired percentile lies between them. For nearest values using a rounding method, see percentileDisc.
   * @param inputs A numeric expression.
   * @param percentile A numeric value between 0.0 and 1.0
   * @return A Double.
   */
  @LynxProcedure(name = "percentileCont")
  def percentileCont(inputs: LynxList, percentile: LynxDouble): LynxDouble ={ // TODO implement it.
    LynxDouble(0)
  }

  // percentileDisc, stDev, stDevP

  /**
   *
   * @param inputs
   * @return
   */
  @LynxProcedure(name = "sum")
  def sum(inputs: LynxList): Any = {
    val dropNull = inputs.value.filterNot(LynxNull.equals)
    val firstIsNum = dropNull.headOption.map{
      case _ :LynxNumber => true
      case _ :LynxDuration => false
      case v :LynxValue => throw LynxProcedureException(s"sum() can only handle numerical values, duration, and null. Got ${v}")
    }
    if (firstIsNum.isDefined) {
      var numSum = 0.0
      var durSum = Duration.ZERO
      dropNull.foreach{ v =>
        if (v.isInstanceOf[LynxNumber] || v.isInstanceOf[LynxDuration]) {
          if (v.isInstanceOf[LynxNumber] == firstIsNum.get) {
            if (firstIsNum.get) { numSum += v.asInstanceOf[LynxNumber].number.doubleValue()}
            else { durSum = durSum.plus(v.asInstanceOf[LynxDuration].duration)}
          } else { throw LynxProcedureException("sum() cannot mix number and duration")}
        } else { throw LynxProcedureException(s"sum() can only handle numerical values, duration, and null. Got ${v}")}
      }
      if (firstIsNum.get) LynxDouble(numSum) else LynxDuration(durSum)
    } else { LynxNull }
  }

  @LynxProcedure(name = "power")
  def power(x: LynxInteger, n: LynxInteger): Int = {
    math.pow(x.value, n.value).toInt
  }

  @LynxProcedure(name = "date")
  def date(inputs: LynxValue): LynxDate = {
    LynxDateUtil.parse(inputs).asInstanceOf[LynxDate]
  }

  @LynxProcedure(name = "date")
  def date(): LynxDate = {
    LynxDateUtil.now()
  }


  @LynxProcedure(name = "datetime")
  def datetime(inputs: LynxValue): LynxDateTime = {
    LynxDateTimeUtil.parse(inputs).asInstanceOf[LynxDateTime]
  }

  @LynxProcedure(name = "datetime")
  def datetime(): LynxDateTime = {
    LynxDateTimeUtil.now()
  }

  @LynxProcedure(name = "localdatetime")
  def localDatetime(inputs: LynxValue): LynxLocalDateTime = {
    LynxLocalDateTimeUtil.parse(inputs).asInstanceOf[LynxLocalDateTime]
  }

  @LynxProcedure(name = "localdatetime")
  def localDatetime(): LynxLocalDateTime = {
    LynxLocalDateTimeUtil.now()
  }

  @LynxProcedure(name = "time")
  def time(inputs: LynxValue): LynxTime = {
    LynxTimeUtil.parse(inputs).asInstanceOf[LynxTime]
  }

  @LynxProcedure(name = "time")
  def time(): LynxTime = {
    LynxTimeUtil.now()
  }

  @LynxProcedure(name = "localtime")
  def localTime(inputs: LynxValue): LynxLocalTime = {
    LynxLocalTimeUtil.parse(inputs).asInstanceOf[LynxLocalTime]
  }

  @LynxProcedure(name = "localtime")
  def localTime(): LynxLocalTime = {
    LynxLocalTimeUtil.now()
  }

  @LynxProcedure(name="duration")
  def duration(input: LynxValue): LynxDuration = {
    input match {
      case LynxString(v) => LynxDurationUtil.parse(v)
      case LynxMap(v) => LynxDurationUtil.parse(v.asInstanceOf[Map[String, LynxNumber]].mapValues(_.number.doubleValue()))
    }
  }

  // math functions
  @LynxProcedure(name = "abs")
  def abs(x: LynxNumber): LynxNumber = {
    x match {
      case i: LynxInteger => LynxInteger(math.abs(i.value))
      case d: LynxDouble => LynxDouble(math.abs(d.value))
    }
  }

  @LynxProcedure(name = "ceil")
  def ceil(x: LynxNumber): Double = {
    math.ceil(x.number.doubleValue())
  }

  @LynxProcedure(name = "floor")
  def floor(x: LynxNumber): Double = {
    math.floor(x.number.doubleValue())
  }

  @LynxProcedure(name = "rand")
  def rand(): Double = {
    math.random()
  }

  @LynxProcedure(name = "round")
  def round(x: LynxNumber): Long = {
    math.round(x.number.doubleValue())
  }

  @LynxProcedure(name = "round")
  def round(x: LynxNumber, precision: LynxInteger): Double = {
    val base = math.pow(10, precision.value)
    math.round(base * x.number.doubleValue()).toDouble / base
  }

  @LynxProcedure(name = "sign")
  def sign(x: LynxNumber): Double = {
    math.signum(x.number.doubleValue())
  }

  @LynxProcedure(name = "e")
  def e(): Double = {
    Math.E
  }

  @LynxProcedure(name = "exp")
  def exp(x: LynxNumber): Double = {
    math.exp(x.number.doubleValue())
  }

  @LynxProcedure(name = "log")
  def log(x: LynxNumber): Double = {
    math.log(x.number.doubleValue())
  }

  @LynxProcedure(name = "log10")
  def log10(x: LynxNumber): Double = {
    math.log10(x.number.doubleValue())
  }

  @LynxProcedure(name = "sqrt")
  def sqrt(x: LynxNumber): Double = {
    math.sqrt(x.number.doubleValue())
  }

  @LynxProcedure(name = "acos")
  def acos(x: LynxNumber): Double = {
    math.acos(x.number.doubleValue())
  }

  @LynxProcedure(name = "asin")
  def asin(x: LynxNumber): Double = {
    math.asin(x.number.doubleValue())
  }

  @LynxProcedure(name = "atan")
  def atan(x: LynxNumber): Double = {
    math.atan(x.number.doubleValue())
  }

  @LynxProcedure(name = "atan2")
  def atan2(x: LynxNumber, y: LynxNumber): Double = {
    math.atan2(x.number.doubleValue(), y.number.doubleValue())
  }

  @LynxProcedure(name = "cos")
  def cos(x: LynxNumber): Double = {
    math.cos(x.number.doubleValue())
  }

  @LynxProcedure(name = "cot")
  def cot(x: LynxNumber): Double = {
    1.0 / math.tan(x.number.doubleValue())
  }

  @LynxProcedure(name = "degrees")
  def degrees(x: LynxNumber): Double = {
    math.toDegrees(x.number.doubleValue())
  }

  @LynxProcedure(name = "haversin")
  def haversin(x: LynxNumber): Double = {
    (1.0d - math.cos(x.number.doubleValue())) / 2
  }

  @LynxProcedure(name = "pi")
  def pi(): Double = {
    Math.PI
  }

  @LynxProcedure(name = "radians")
  def radians(x: LynxNumber): Double = {
    math.toRadians(x.number.doubleValue())
  }

  @LynxProcedure(name = "sin")
  def sin(x: LynxNumber): Double = {
    math.sin(x.number.doubleValue())
  }

  @LynxProcedure(name = "tan")
  def tan(x: LynxNumber): Double = {
    math.tan(x.number.doubleValue())
  }

  // list function

  /**
   * Returns a list containing the string representations
   * for all the property names of a node, relationship, or map.
   * @param x A node, relationship, or map
   * @return property names
   */
  @LynxProcedure(name = "keys")
  def keys(x: LynxValue): List[String] = x match {
    case n: LynxNode => n.keys.toList.map(_.value)
    case r: LynxRelationship => r.keys.toList.map(_.value)
    case m: LynxMap => m.value.keys.toList
    case _ => throw new LynxProcedureException("keys() can only used on node, relationship and map.")
  }

  /**
   * Returns a list containing the string representations for all the labels of a node.
   * @param x The node
   * @return labels
   */
  @LynxProcedure(name = "labels")
  def labels(x: LynxNode): Seq[String] = x.labels.map(_.value)

  /**
   * Returns a list containing all the nodes in a path.
   * @param inputs path
   * @return nodes
   */
  @LynxProcedure(name = "nodes")
  def nodes(inputs: LynxList): List[LynxNode] = {
    def fetchNodeFromList(list: LynxList): LynxNode = {
      list.value.filter(item => item.isInstanceOf[LynxNode]).head.asInstanceOf[LynxNode]
    }

    def fetchListFromList(list: LynxList): LynxList = {
      list.value.filter(item => item.isInstanceOf[LynxList]).head.asInstanceOf[LynxList]
    }

    val list = fetchListFromList(inputs)
    if (list.value.nonEmpty) List(fetchNodeFromList(inputs)) ++ nodes(fetchListFromList(inputs))
    else List(fetchNodeFromList(inputs))
  }

  /**
   * Returns a list comprising all integer values within a specified range. TODO
   * @param inputs
   * @return
   */
  @LynxProcedure(name = "range")
  def range(start: LynxInteger, end: LynxInteger): LynxList = range(start, end, LynxInteger(1))

  @LynxProcedure(name = "range")
  def range(start: LynxInteger, end: LynxInteger, step: LynxInteger): LynxList =
    LynxList((start.value to end.value by step.value).toList map LynxInteger)

  /**
   * Returns a list containing all the relationships in a path.
   * @param inputs path
   * @return relationships
   */
  @LynxProcedure(name = "relationships")
  def relationships(inputs: LynxList): List[LynxRelationship] = {
    val list: LynxList = inputs.value.tail.head.asInstanceOf[LynxList]
    list.value.filter(value => value.isInstanceOf[LynxRelationship]).asInstanceOf[List[LynxRelationship]].reverse
  }

  // string functions
  @LynxProcedure(name = "left")
  def left(x: LynxString, endIndex: LynxInteger): String = {
    val str = x.value
    if (endIndex.value.toInt < str.length) str.substring(0, endIndex.value.toInt)
    else str
  }

  @LynxProcedure(name = "right")
  def right(x: LynxString, endIndex: LynxInteger): String = {
    val str = x.value
    if (endIndex.value.toInt < str.length) str.substring(endIndex.value.toInt - 1)
    else str
  }

  @LynxProcedure(name = "ltrim")
  def ltrim(x: LynxString): String = {
    val str = x.value
    if (str == "" || str == null) str
    else x.value.replaceAll(s"^[  ]+", "")
  }

  @LynxProcedure(name = "rtrim")
  def rtrim(x: LynxString): String = {
    val str = x.value
    if (str == "" || str == null) str
    else x.value.replaceAll(s"[　 ]+$$", "")
  }

  @LynxProcedure(name = "trim")
  def trim(x: LynxString): String = {
    val str = x.value
    if (str == "" || str == null) str
    else str.trim
  }

  @LynxProcedure(name = "replace")
  def replace(x: LynxString, search: LynxString, replace: LynxString): String = {
    val str = x.value
    if (str == "" || str == null) str
    else str.replaceAll(search.value, replace.value)
  }

  @LynxProcedure(name = "reverse")
  def reverse(x: LynxString): String = {
    val str = x.value
    if (str == "" || str == null) str
    else str.reverse
  }

  @LynxProcedure(name = "split")
  def split(x: LynxString, regex: LynxString): Array[String] = {
    val str = x.value
    if (str == "" || str == null) Array(str)
    else str.split(regex.value)
  }

  @LynxProcedure(name = "substring")
  def substring(x: LynxString, left: LynxInteger, length: LynxInteger): String = {
    val str = x.value
    if (str == "" || str == null) str
    else {
      if (left.value.toInt + length.value.toInt < str.length)
        str.substring(left.value.toInt, left.value.toInt + length.value.toInt)
      else str.substring(left.value.toInt)
    }
  }

  @LynxProcedure(name = "substring")
  def substring(x: LynxString, left: LynxInteger): String = {
    val str = x.value
    if (str == "" || str == null) str
    else str.substring(left.value.toInt)
  }

  @LynxProcedure(name = "toLower")
  def toLower(x: LynxString): String = {
    x.value.toLowerCase
  }

  @LynxProcedure(name = "toUpper")
  def toUpper(x: LynxString): String = {
    x.value.toUpperCase
  }

  @LynxProcedure(name = "toString")
  def toString(x: LynxValue): String = x match {
//    case dr: LynxDuration => dr.toString
    case _ => x.value.toString
  }
}

