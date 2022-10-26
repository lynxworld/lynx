package org.grapheco.lynx.procedure

import org.grapheco.lynx.{LynxException, LynxType}

case class ProcedureException(msg: String) extends LynxException {
  override def getMessage: String = msg
}

case class UnknownProcedureException(prefix: List[String], name: String) extends LynxException {
  override def getMessage: String = s"unknown procedure: ${(prefix :+ name).mkString(".")}"
}

case class WrongArgumentException(argName: String, expectedTypes: Seq[LynxType], actualTypes: Seq[LynxType]) extends LynxException {
  override def getMessage: String = s"Wrong argument of $argName, expected: ${expectedTypes.mkString(", ")}, actual: ${actualTypes.mkString(", ")}."
}

case class WrongNumberOfArgumentsException(signature: String, sizeExpected: Int, sizeActual: Int) extends LynxException {
  override def getMessage: String = s"Wrong number of arguments of $signature(), expected: $sizeExpected, actual: ${sizeActual}."
}
