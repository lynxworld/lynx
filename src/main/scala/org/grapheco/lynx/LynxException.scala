package org.grapheco.lynx

trait LynxException extends RuntimeException

object LynxException {
  def apply(message: String): LynxException = new LynxException {
    override def getMessage: String = message
  }
}
