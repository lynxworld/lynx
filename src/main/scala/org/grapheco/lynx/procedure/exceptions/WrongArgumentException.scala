package org.grapheco.lynx.procedure.exceptions

import org.grapheco.lynx.{LynxException, LynxType}

/**
 * @ClassName WrongArgumentException
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/20
 * @Version 0.1
 */
case class WrongArgumentException(argName: String, expectedTypes: Seq[LynxType], actualTypes: Seq[LynxType]) extends LynxException {
  override def getMessage: String = s"Wrong argument of $argName, expected: ${expectedTypes.mkString(", ")}, actual: ${actualTypes.mkString(", ")}."
}
