package org.grapheco.lynx.procedure

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.func.{LynxProcedure, LynxProcedureArgument}
import org.grapheco.lynx.types.{LynxValue, TypeSystem}
import org.grapheco.lynx.LynxType

import scala.collection.mutable

/**
 * @ClassName DefaultProcedureRegistry
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/20
 * @Version 0.1
 */
class DefaultProcedureRegistry(types: TypeSystem, classes: Class[_]*) extends ProcedureRegistry with LazyLogging {
  val procedures = mutable.Map[(String, Int), CallableProcedure]()

  classes.foreach(registerAnnotatedClass)

  def registerAnnotatedClass(clazz: Class[_]): Unit = {
    val host = clazz.newInstance()

    clazz.getDeclaredMethods.foreach{ method =>
      val annotation = method.getAnnotation(classOf[LynxProcedure])
      if (annotation != null) {
        val inputs = method.getParameters.map{ parameter =>
          // LynxProcedureArgument("xx") or parameter name.
          val paraAnnotation = Option(parameter.getAnnotation(classOf[LynxProcedureArgument]))
          val name = paraAnnotation.map(_.name()).getOrElse(parameter.getName)
          name -> types.typeOf(parameter.getType)
        }
        val outputs = Seq("value" -> types.typeOf(method.getReturnType))
        register(annotation.name(), inputs, outputs, args => types.wrap(method.invoke(host, args: _*)))
      }
    }
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
