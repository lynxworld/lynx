package org.grapheco.lynx.parser

import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.ast.semantics.SemanticState

trait QueryParser {
  def parse(query: String): (Statement, Map[String, Any], SemanticState)
}
