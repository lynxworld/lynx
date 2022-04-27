package org.grapheco.lynx.physical

import org.grapheco.lynx.types.structural.LynxId

sealed trait NodeInputRef

case class StoredNodeInputRef(id: LynxId) extends NodeInputRef

case class ContextualNodeInputRef(varName: String) extends NodeInputRef
