package org.grapheco.lynx.types

import org.grapheco.lynx.LynxException

/**
 * @ClassName InvalidValueException
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class InvalidValueException(unknown: Any) extends LynxException
