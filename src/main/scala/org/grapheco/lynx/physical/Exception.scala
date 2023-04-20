package org.grapheco.lynx.physical

import org.grapheco.lynx.LynxException
import org.opencypher.v9_0.expressions.LogicalVariable

/**
 * @ClassName SyntaxErrorException
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
case class SyntaxErrorException(msg: String) extends LynxException

case class UnresolvableVarException(var0: Option[LogicalVariable]) extends LynxException

case class ExecuteException(msg: String) extends LynxException
