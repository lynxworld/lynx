 package org.grapheco.lynx

import java.util.regex.Pattern
import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.func.{LynxProcedure, LynxProcedureArgument}
import org.grapheco.lynx.util.{LynxDateTimeUtil, LynxDateUtil, LynxDurationUtil, LynxLocalDateTimeUtil, LynxLocalTimeUtil, LynxTimeUtil}
import org.opencypher.v9_0.expressions.{Expression, FunctionInvocation}
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.opencypher.v9_0.frontend.phases.{BaseContext, BaseState, Condition, Phase}
import org.opencypher.v9_0.util.{InputPosition, Rewriter, bottomUp, inSequence}

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
  override def getMessage: String = s"Wrong number of arguments of $signature(), expected: $sizeExpected, actual: $sizeActual"
}

case class WrongArgumentException(argName: String, expectedType: LynxType, actualType: LynxType) extends LynxException {
  override def getMessage: String = s"Wrong argument of $argName, expected: $expectedType, actual: ${actualType}"
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

case class FunctionMapper(runnerContext: CypherRunnerContext) extends Phase[BaseContext, BaseState, BaseState] {
  override def phase: CompilationPhase = AST_REWRITE

  override def description: String = "map functions to their procedure implementations"

  override def process(from: BaseState, ignored: BaseContext): BaseState = {
    val rewriter = inSequence(
      bottomUp(Rewriter.lift {
        case func: FunctionInvocation => ProcedureExpression(func)(runnerContext)
      }))
    val newStatement = from.statement().endoRewrite(rewriter)
    from.withStatement(newStatement)
  }

  override def postConditions: Set[Condition] = Set.empty
}

class DefaultProcedures {

  val booleanPattern = Pattern.compile("true|false", Pattern.CASE_INSENSITIVE)
  val numberPattern = Pattern.compile("-?[0-9]+.?[0-9]+")

  @LynxProcedure(name = "lynx")
  def lynx(): String = {
    "lynx-0.3"
  }

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

  @LynxProcedure(name = "relationships")
  def relationships(inputs: LynxList): List[LynxRelationship] = {
    val list: LynxList = inputs.value.tail.head.asInstanceOf[LynxList]
    list.value.filter(value => value.isInstanceOf[LynxRelationship]).asInstanceOf[List[LynxRelationship]].reverse
  }

  //user should opt the count implementation at their own project
  @LynxProcedure(name = "count")
  def count(inputs: LynxList): Int = {
    inputs.value.size
  }

  @LynxProcedure(name = "length")
  def length(inputs: LynxList): Int = {
    val list: LynxList = inputs.value.tail.head.asInstanceOf[LynxList]
    list.value.filter(value => value.isInstanceOf[LynxRelationship]).length
  }

  @LynxProcedure(name = "size")
  def size(input: LynxValue): Int = {
    input match {
      case l: LynxList => l.value.size
      case s: LynxString => s.value.size
    }
  }

  @LynxProcedure(name = "sum")
  def sum(inputs: LynxList): LynxNumber = {
    inputs.value.map(_.asInstanceOf[LynxNumber]).reduce((a, b) => a + b)
  }

  @LynxProcedure(name = "avg")
  def avg(inputs: LynxList): Any = {
    val cnt = inputs.value.size
    if (cnt==0) return null
    inputs.value match {
      case l: List[LynxNumber] => l.map(_.number.doubleValue()).reduce((a, b) => a + b) / cnt
      case l: List[LynxDuration] => l.map(_.value).reduce((a, b) => a.plus(b)).dividedBy(cnt)
    }
  }

  @LynxProcedure(name = "collect")
  def collect(inputs: LynxList): LynxList = {
    inputs
  }

  @LynxProcedure(name = "max")
  def max(inputs: LynxList): LynxNumber = {
    inputs.value.map(_.asInstanceOf[LynxNumber]).reduce((a, b) => if (a > b) a else b)
  }

  @LynxProcedure(name = "min")
  def min(inputs: LynxList): LynxNumber = {
    inputs.value.map(_.asInstanceOf[LynxNumber]).reduce((a, b) => if (a > b) b else a)
  }

  @LynxProcedure(name = "power")
  def power(x: LynxInteger, n: LynxInteger): Int = {
    math.pow(x.value, n.value).toInt
  }

  @LynxProcedure(name="date")
  def date(inputs: LynxValue): LynxDate = {
    LynxDateUtil.parse(inputs).asInstanceOf[LynxDate]
  }

  @LynxProcedure(name="date")
  def date(): LynxDate = {
    LynxDateUtil.now()
  }


  @LynxProcedure(name="datetime")
  def datetime(inputs: LynxValue): LynxDateTime = {
    LynxDateTimeUtil.parse(inputs).asInstanceOf[LynxDateTime]
  }

  @LynxProcedure(name="datetime")
  def datetime(): LynxDateTime = {
    LynxDateTimeUtil.now()
  }

  @LynxProcedure(name="localdatetime")
  def localDatetime(inputs: LynxValue): LynxLocalDateTime = {
    LynxLocalDateTimeUtil.parse(inputs).asInstanceOf[LynxLocalDateTime]
  }

  @LynxProcedure(name="localdatetime")
  def localDatetime(): LynxLocalDateTime = {
    LynxLocalDateTimeUtil.now()
  }

  @LynxProcedure(name="time")
  def time(inputs: LynxValue): LynxTime = {
    LynxTimeUtil.parse(inputs).asInstanceOf[LynxTime]
  }

  @LynxProcedure(name="time")
  def time(): LynxTime = {
    LynxTimeUtil.now()
  }

  @LynxProcedure(name="localtime")
  def localTime(inputs: LynxValue): LynxLocalTime = {
    LynxLocalTimeUtil.parse(inputs).asInstanceOf[LynxLocalTime]
  }

  @LynxProcedure(name="localtime")
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
  @LynxProcedure(name= "abs")
  def abs(x: LynxNumber): LynxNumber = {
    x match {
      case i: LynxInteger => LynxInteger(math.abs(i.value))
      case d: LynxDouble => LynxDouble(math.abs(d.value))
    }
  }

  @LynxProcedure(name= "ceil")
  def ceil(x: LynxNumber): Double = {
    math.ceil(x.number.doubleValue())
  }

  @LynxProcedure(name= "floor")
  def floor(x: LynxNumber): Double = {
   math.floor(x.number.doubleValue())
  }

  @LynxProcedure(name= "rand")
  def rand(): Double = {
    math.random()
  }

  @LynxProcedure(name= "round")
  def round(x: LynxNumber): Long = {
    math.round(x.number.doubleValue())
  }

  @LynxProcedure(name= "round")
  def round(x: LynxNumber, precision: LynxInteger): Double = {
    val base = math.pow(10, precision.value)
    math.round(base * x.number.doubleValue()).toDouble / base
  }

  @LynxProcedure(name= "sign")
  def sign(x: LynxNumber): Double = {
   math.signum(x.number.doubleValue())
  }

  @LynxProcedure(name= "e")
  def e(): Double = {
   Math.E
  }

  @LynxProcedure(name= "exp")
  def exp(x: LynxNumber): Double = {
    math.exp(x.number.doubleValue())
  }

  @LynxProcedure(name= "log")
  def log(x: LynxNumber): Double = {
   math.log(x.number.doubleValue())
  }

  @LynxProcedure(name= "log10")
  def log10(x: LynxNumber): Double = {
    math.log10(x.number.doubleValue())
  }

  @LynxProcedure(name= "sqrt")
  def sqrt(x: LynxNumber): Double = {
   math.sqrt(x.number.doubleValue())
  }

  @LynxProcedure(name= "acos")
  def acos(x: LynxNumber): Double = {
   math.acos(x.number.doubleValue())
  }

  @LynxProcedure(name= "asin")
  def asin(x: LynxNumber): Double = {
    math.asin(x.number.doubleValue())
  }

  @LynxProcedure(name= "atan")
  def atan(x: LynxNumber): Double = {
   math.atan(x.number.doubleValue())
  }

  @LynxProcedure(name= "atan2")
  def atan2(x: LynxNumber, y: LynxNumber): Double = {
    math.atan2(x.number.doubleValue(), y.number.doubleValue())
  }

  @LynxProcedure(name= "cos")
  def cos(x: LynxNumber): Double = {
   math.cos(x.number.doubleValue())
  }

  @LynxProcedure(name= "cot")
  def cot(x: LynxNumber): Double = {
    1.0 / math.tan(x.number.doubleValue())
  }

  @LynxProcedure(name= "degrees")
  def degrees(x: LynxNumber): Double = {
    math.toDegrees(x.number.doubleValue())
  }

  @LynxProcedure(name= "haversin")
  def haversin(x: LynxNumber): Double = {
    (1.0d - math.cos(x.number.doubleValue())) / 2
  }

  @LynxProcedure(name= "pi")
  def pi(): Double = {
   Math.PI
  }

  @LynxProcedure(name= "radians")
  def radians(x: LynxNumber): Double = {
    math.toRadians(x.number.doubleValue())
  }

  @LynxProcedure(name= "sin")
  def sin(x: LynxNumber): Double = {
    math.sin(x.number.doubleValue())
  }

  @LynxProcedure(name= "tan")
  def tan(x: LynxNumber): Double = {
   math.tan(x.number.doubleValue())
  }

  // list function
  @LynxProcedure(name= "labels")
  def labels(x: LynxNode): Seq[String] = {
    x.labels
  }

  // scalar functions
  @LynxProcedure(name= "id")
  def id(x: LynxValue): Long = {
    x match {
      case n: LynxNode => n.id.value.asInstanceOf[Long]
      case r: LynxRelationship => r.id.value.asInstanceOf[Long]
      case _ => throw new LynxFunctionException("id can only used on node and relationship")
    }
  }

  @LynxProcedure(name= "toInteger")
  def toInteger(x: LynxValue): LynxValue = {
    x match {
      case n: LynxNumber => LynxInteger(n.number.intValue())
      case r: LynxString => {
        val str = r.value
        val res = numberPattern.matcher(str)
        if (res.matches()) {
          LynxInteger(str.toDouble.toInt)
        }
        else LynxNull
      }
      case _ => throw new LynxFunctionException("toInteger conversion failure")
    }
  }

  @LynxProcedure(name= "toFloat")
  def toFloat(x: LynxValue): LynxValue = {
    x match {
      case n: LynxNumber => LynxDouble(n.number.floatValue())
      case r: LynxString => {
        val str = r.value
        val res = numberPattern.matcher(str)
        if (res.matches()) LynxDouble(str.toDouble)
        else LynxNull
      }
      case _ => throw new LynxFunctionException("toFloat conversion failure")
    }
  }

  @LynxProcedure(name= "toBoolean")
  def toBoolean(x: LynxValue): LynxValue = {
    x match {
      case r: LynxString => {
        val str = r.value
        val res = booleanPattern.matcher(str)
        if (res.matches()) LynxValue(str.toBoolean)
        else LynxNull
      }
      case _ => throw new LynxFunctionException("toBoolean conversion failure")
    }
  }

  @LynxProcedure(name= "type")
  def getType(x: LynxValue): LynxValue = {
    x match {
      case r: LynxRelationship => {
        val t = r.relationType
        if (t.isDefined) LynxValue(t.get)
        else LynxNull
      }
      case _ => throw new LynxFunctionException("type can only used on relationship")
    }
  }


  // string functions
  @LynxProcedure(name= "left")
  def left(x: LynxString, endIndex: LynxInteger): String = {
    val str = x.value
    if (endIndex.value.toInt < str.length) str.substring(0, endIndex.value.toInt)
    else str
  }

  @LynxProcedure(name= "right")
  def right(x: LynxString, endIndex: LynxInteger): String = {
    val str = x.value
    if (endIndex.value.toInt < str.length) str.substring(endIndex.value.toInt - 1)
    else str
  }

  @LynxProcedure(name= "ltrim")
  def ltrim(x: LynxString): String = {
    val str = x.value
    if (str == "" || str == null) str
    else x.value.replaceAll(s"^[  ]+", "")
  }

  @LynxProcedure(name= "rtrim")
  def rtrim(x: LynxString): String = {
    val str = x.value
    if (str == "" || str == null) str
    else x.value.replaceAll(s"[　 ]+$$", "")
}

  @LynxProcedure(name= "trim")
  def trim(x: LynxString): String = {
    val str = x.value
    if (str == "" || str == null) str
    else str.trim
  }

  @LynxProcedure(name= "replace")
  def replace(x: LynxString, search: LynxString, replace: LynxString): String = {
    val str = x.value
    if (str == "" || str == null) str
    else str.replaceAll(search.value, replace.value)
  }

  @LynxProcedure(name= "reverse")
  def reverse(x: LynxString): String = {
    val str = x.value
    if (str == "" || str == null) str
    else str.reverse
  }

  @LynxProcedure(name= "split")
  def split(x: LynxString, regex: LynxString): Array[String] = {
    val str = x.value
    if (str == "" || str == null) Array(str)
    else str.split(regex.value)
  }

  @LynxProcedure(name= "substring")
  def substring(x: LynxString, left: LynxInteger, length: LynxInteger): String = {
    val str = x.value
    if (str == "" || str == null) str
    else {
      if (left.value.toInt + length.value.toInt < str.length)
        str.substring(left.value.toInt, left.value.toInt + length.value.toInt)
      else str.substring(left.value.toInt)
    }
  }

  @LynxProcedure(name= "substring")
  def substring(x: LynxString, left: LynxInteger): String = {
    val str = x.value
    if (str == "" || str == null) str
    else str.substring(left.value.toInt)
  }

  @LynxProcedure(name= "toLower")
  def toLower(x: LynxString): String = {
    x.value.toLowerCase
  }

  @LynxProcedure(name= "toUpper")
  def toUpper(x: LynxString): String = {
    x.value.toUpperCase
  }

  @LynxProcedure(name= "toString")
  def toString(x: LynxValue): String = {
    x.value.toString
  }
}

class LynxFunctionException(msg: String) extends LynxException{
  override def getMessage: String = msg
}