package org.grapheco.lynx.procedure.exceptions

import org.grapheco.lynx.LynxException

/**
 * @ClassName LynxProcedureException
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/20
 * @Version 0.1
 */
case class LynxProcedureException(msg: String) extends LynxException {
  override def getMessage: String = msg
}
