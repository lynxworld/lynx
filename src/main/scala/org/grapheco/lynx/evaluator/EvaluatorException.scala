package org.grapheco.lynx.evaluator

import org.grapheco.lynx.LynxException
/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:15 2022/7/14
 * @Modified By:
 */
case class EvaluatorException(msg: String) extends LynxException {
  override def getMessage: String = msg
}

case class EvaluatorTypeMismatch(actualType: String, expectedType: String) extends LynxException {
  override def getMessage: String = s"Type Mismatch, expected ${expectedType}, actual ${actualType}"
}

case class EvaluatorOptUnsupported(type1: String, type2: String, opt: String) extends LynxException {
  override def getMessage: String = s"The operator $opt cannot be used between type $type1 and type $type2."
}