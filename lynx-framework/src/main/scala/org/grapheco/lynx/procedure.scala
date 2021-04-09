package org.grapheco.lynx

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.func.{LynxProcedure, LynxProcedureArgument}

import scala.collection.mutable

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

          argName -> types.typeOf(par.getType.asInstanceOf[Class[Any]])
        })

        register(an.name(), inputs, Seq.empty, (args) => types.wrap(met.invoke(host, args)))
      }
    })
  }

  def register(name: String, procedure: CallableProcedure): Unit = {
    procedures(name) = procedure
    logger.debug(s"registered procedure: $name")
  }

  def register(name: String, inputs0: Seq[(String, LynxType)], outputs0: Seq[(String, LynxType)], call0: (Seq[LynxType]) => LynxValue): Unit = {
    register(name, new CallableProcedure() {
      override val inputs: Seq[(String, LynxType)] = inputs0
      override val outputs: Seq[(String, LynxType)] = outputs0

      override def call(args: Seq[LynxValue], ctx: ExecutionContext): Iterable[Seq[LynxValue]] = ???
    })
  }

  override def getProcedure(prefix: List[String], name: String): Option[CallableProcedure] = procedures.get((prefix :+ name).mkString("."))
}

class DefaultFunctions {
  @LynxProcedure(name = "lynx")
  def lynx(): String = {
    "lynx-0.3"
  }
}

case class UnknownProcedureException(prefix: List[String], name: String) extends LynxException {
  override def getMessage: String = s"unknown procedure: ${(prefix :+ name).mkString(".")}"
}

case class WrongNumberOfArgumentsException(signature: String, sizeExpected: Int, sizeActual: Int) extends LynxException {
  override def getMessage: String = s"Wrong number of arguments of $signature(), expected: $sizeExpected, actual: $sizeActual"
}

case class WrongArgumentException(argName: String, expectedType: LynxType, actualValue: LynxValue) extends LynxException {
  override def getMessage: String = s"Wrong argument of $argName, expected: $expectedType, actual: ${actualValue.cypherType}"
}