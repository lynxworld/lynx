package org.grapheco.lynx.procedure

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.LynxType
import org.grapheco.lynx.func.{LynxProcedure, LynxProcedureArgument}
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.{LynxValue, TypeSystem}

import scala.collection.mutable

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 13:47 2022/6/28
 * @Modified By:
 */
class WithGraphModelProcedureRegistry(types: TypeSystem,
                                      scalarFunctions: ScalarFunctions,
                                      classes: Class[_]*) extends ProcedureRegistry with LazyLogging {
  val procedures = mutable.Map[(String, Int), CallableProcedure]()

  classes.foreach(registerAnnotatedClass)
  registerScalarFunctions

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

  def registerScalarFunctions: Unit = {
    scalarFunctions.getClass.getDeclaredMethods.foreach{ method =>
      val annotation = method.getAnnotation(classOf[LynxProcedure])
      if (annotation != null) {
        val inputs = method.getParameters.map{ parameter =>
          val paraAnnotation = Option(parameter.getAnnotation(classOf[LynxProcedureArgument]))
          val name = paraAnnotation.map(_.name()).getOrElse(parameter.getName)
          name -> types.typeOf(parameter.getType)
        }
        val outputs = Seq("value" -> types.typeOf(method.getReturnType))
        register(annotation.name(), inputs, outputs, args => types.wrap(method.invoke(scalarFunctions, args: _*)))
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
      override def call(args: Seq[LynxValue]): LynxValue = {
        name match {
          case "coalesce" => LynxValue(call0(Seq(LynxList(args.toList))))
          case _ => LynxValue(call0(args))
        }
      }
    })
  }

  override def getProcedure(prefix: List[String], name: String, argsLength: Int): Option[CallableProcedure] = {
    name match {
      case "coalesce" => procedures.get(((prefix :+ name).mkString("."), 1))
      case _ => procedures.get(((prefix :+ name).mkString("."), argsLength))
    }
  }
}
