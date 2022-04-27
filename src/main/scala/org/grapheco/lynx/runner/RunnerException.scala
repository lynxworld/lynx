package org.grapheco.lynx.runner

import org.grapheco.lynx.LynxException

case class ConstrainViolatedException(msg: String) extends LynxException {
  override def getMessage: String = msg
}

case class NoIndexManagerException(msg: String) extends LynxException {
  override def getMessage: String = msg
}

case class ProcedureUnregisteredException(msg: String) extends LynxException {
  override def getMessage: String = msg
}

case class ParsingException(msg: String) extends LynxException {
  override def getMessage: String = msg
}