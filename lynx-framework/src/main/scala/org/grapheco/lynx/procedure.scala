package org.grapheco.lynx

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.func.{LynxProcedure, LynxProcedureArgument}
import org.opencypher.v9_0.expressions.{Expression, FunctionInvocation, FunctionName, Namespace}
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.opencypher.v9_0.frontend.phases.{BaseContext, BaseState, Condition, Phase}
import org.opencypher.v9_0.util.{InputPosition, Rewriter, bottomUp, inSequence}

import scala.collection.mutable
import org.opencypher.v9_0.util.symbols.CTAny

trait CallableProcedure {
  val inputs: Seq[(String, LynxType)]
  val outputs: Seq[(String, LynxType)]

  def call(args: Seq[LynxValue], ctx: ExecutionContext): LynxValue

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

trait CallableAggregationProcedure extends CallableProcedure {
  val outputName: String
  val outputValueType: LynxType

  final override val inputs: Seq[(String, LynxType)] = Seq("values" -> CTAny)
  final override val outputs: Seq[(String, LynxType)] = Seq(outputName -> outputValueType)

  final override def call(args: Seq[LynxValue], ctx: ExecutionContext): LynxValue = {
    collect(args.head)
    LynxNull //TODO CallableAggregationProcedure
  }

  def collect(value: LynxValue): Unit

  def value(): LynxValue
}

trait ProcedureRegistry {
  def getProcedure(prefix: List[String], name: String): Option[CallableProcedure]
}

class DefaultProcedureRegistry(types: TypeSystem, classes: Class[_]*) extends ProcedureRegistry with LazyLogging {
  val procedures = mutable.Map[String, CallableProcedure]()

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

  def register(name: String, procedure: CallableProcedure): Unit = {
    procedures(name) = procedure
    logger.debug(s"registered procedure: ${procedure.signature(name)}")
  }

  def register(name: String, inputs0: Seq[(String, LynxType)], outputs0: Seq[(String, LynxType)], call0: (Seq[LynxValue]) => LynxValue): Unit = {
    register(name, new CallableProcedure() {
      override val inputs: Seq[(String, LynxType)] = inputs0
      override val outputs: Seq[(String, LynxType)] = outputs0

      override def call(args: Seq[LynxValue], ctx: ExecutionContext): LynxValue = LynxValue(call0(args))
    })
  }

  override def getProcedure(prefix: List[String], name: String): Option[CallableProcedure] = procedures.get((prefix :+ name).mkString("."))
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

case class ProcedureExpression(val funcInov: FunctionInvocation)(implicit runnerContext: CypherRunnerContext) extends Expression {
  val procedure: CallableProcedure = runnerContext.procedureRegistry.getProcedure(funcInov.namespace.parts, funcInov.functionName.name).get
  val args: Seq[Expression] = funcInov.args
  val aggregating: Boolean = funcInov.containsAggregate

  override def position: InputPosition = funcInov.position

  override def productElement(n: Int): Any = funcInov.productElement(n)

  override def productArity: Int = funcInov.productArity

  override def canEqual(that: Any): Boolean = funcInov.canEqual(that)
}

case class FunctionMapper(runnerContext: CypherRunnerContext) extends Phase[BaseContext, BaseState, BaseState] {
  override def phase: CompilationPhase = AST_REWRITE

  override def description: String = "map functions to their procedure implementations"

  override def process(from: BaseState, ignored: BaseContext): BaseState = {
    val rewriter = inSequence(
      bottomUp(Rewriter.lift {
        case func: FunctionInvocation => {
          ProcedureExpression(func)(runnerContext)
        }
      }))
    val newStatement = from.statement().endoRewrite(rewriter)
    from.withStatement(newStatement)
  }

  override def postConditions: Set[Condition] = Set.empty
}

class DefaultProcedures {
  @LynxProcedure(name = "lynx")
  def lynx(): String = {
    "lynx-0.3"
  }
  @LynxProcedure(name = "sum")
  def sum(inputs: Seq[LynxInteger]): Int = {
    inputs.map(_.value.toInt).reduce((a, b) => a + b)
  }
  @LynxProcedure(name = "power")
  def power(x: LynxInteger, n: LynxInteger): Int = {
    math.pow(x.value, n.value).toInt
  }
}