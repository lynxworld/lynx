package org.grapheco.lynx.types

import org.grapheco.lynx.LynxException

/**
 * @ClassName InvalidValueException
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
class TypeException extends LynxException

case class InvalidValueException(unknown: Any) extends TypeException{
  override def getMessage: String = s"Invalid value"
}

case class TypeMismatchException(expected: LynxType, actual: LynxType) extends TypeException{
  override def getMessage: String = s"Type mismatch: expected ${expected}but was ${actual}."
}

case class TypeCompareException(type1: LynxType, type2: LynxType) extends TypeException{
  override def getMessage: String = s"Type $type1 cannot be compared with Type $type2."
}

