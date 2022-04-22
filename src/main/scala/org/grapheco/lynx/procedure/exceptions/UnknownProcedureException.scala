package org.grapheco.lynx.procedure.exceptions

import org.grapheco.lynx.LynxException

/**
 * @ClassName UnknownProcedureException
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/20
 * @Version 0.1
 */
case class UnknownProcedureException(prefix: List[String], name: String) extends LynxException {
  override def getMessage: String = s"unknown procedure: ${(prefix :+ name).mkString(".")}"
}
