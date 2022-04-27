package org.grapheco.lynx.parser

import org.grapheco.lynx.LynxException
import org.opencypher.v9_0.util.spi.MapToPublicExceptions
import org.opencypher.v9_0.util.{CypherException, InputPosition}

/**
 * @ClassName LynxCypherException
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
class LynxCypherException(msg: String, position: InputPosition) extends CypherException(msg, null) with LynxException {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T = ???
}
