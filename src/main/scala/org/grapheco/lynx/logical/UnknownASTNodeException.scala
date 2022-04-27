package org.grapheco.lynx.logical

import org.grapheco.lynx.LynxException
import org.opencypher.v9_0.util.ASTNode

/**
 * @ClassName UnknownASTNodeException
 * @Description
 * @Author Hu Chuan
 * @Date 2022/4/27
 * @Version 0.1
 */
case class UnknownASTNodeException(node: ASTNode) extends LynxException
