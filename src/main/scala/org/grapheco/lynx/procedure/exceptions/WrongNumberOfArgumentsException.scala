package org.grapheco.lynx.procedure.exceptions

import org.grapheco.lynx.LynxException

/**
 * @ClassName WrongNumberOfArgumentsException
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/20
 * @Version 0.1
 */
case class WrongNumberOfArgumentsException(signature: String, sizeExpected: Int, sizeActual: Int) extends LynxException {
  override def getMessage: String = s"Wrong number of arguments of $signature(), expected: $sizeExpected, actual: ${sizeActual}."
}
